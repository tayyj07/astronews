#!/usr/bin/env python3
"""SQLite layer for AstroNews multi-user state.

Schema (v1):
    users               — registry of every chat_id that has interacted with the bot
    user_topics         — current active topic subscriptions, one row per (user, topic)
    topic_events        — append-only audit log of every /add and /remove
    user_digested_urls  — per-user URL dedup, pruned to the last 7 days at write time
    user_notifications  — per-user record of which digest files have been sent

The DB lives in the repo at `state/astronews.db` so it backs up via GitHub
and can be inspected locally with `sqlite3 state/astronews.db`.

All writers must hold the shared flock (/home/astronews/.astronews.lock) so
SQLite + git pushes don't clash with the scraper or notifier.
"""

from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "state" / "astronews.db"

SCHEMA_VERSION = 2
DEDUP_KEEP_DAYS = 7


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    chat_id      INTEGER PRIMARY KEY,
    username     TEXT,
    first_name   TEXT,
    joined_at    TEXT NOT NULL,
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS user_topics (
    chat_id                  INTEGER NOT NULL,
    topic                    TEXT NOT NULL,    -- the user-typed text (display)
    added_at                 TEXT NOT NULL,
    classified_at            TEXT,             -- NULL until the LLM has decomposed into atoms
    classification_rationale TEXT,             -- optional brief LLM note for /retag debugging
    PRIMARY KEY (chat_id, topic),
    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_user_topics_topic ON user_topics(topic);
CREATE INDEX IF NOT EXISTS idx_user_topics_unclassified ON user_topics(classified_at) WHERE classified_at IS NULL;

CREATE TABLE IF NOT EXISTS topic_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER NOT NULL,
    action      TEXT NOT NULL CHECK (action IN ('add', 'remove')),
    topic       TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES users(chat_id)
);
CREATE INDEX IF NOT EXISTS idx_events_chat_time ON topic_events(chat_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_topic ON topic_events(topic);

CREATE TABLE IF NOT EXISTS user_digested_urls (
    chat_id     INTEGER NOT NULL,
    url         TEXT NOT NULL,
    surfaced_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, url),
    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_digested_chat_time ON user_digested_urls(chat_id, surfaced_at);

CREATE TABLE IF NOT EXISTS user_notifications (
    chat_id          INTEGER NOT NULL,
    digest_filename  TEXT NOT NULL,
    sent_at          TEXT NOT NULL,
    PRIMARY KEY (chat_id, digest_filename),
    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
);

-- Atomic facets (tags). Topics decompose into 1-3 atoms via LLM at digest-routine
-- fire time. New atoms are auto-created by the LLM when no existing atom fits.
CREATE TABLE IF NOT EXISTS atoms (
    atom_id      TEXT PRIMARY KEY,                  -- kebab-case id, e.g. 'stablecoins', 'tradfi'
    kind         TEXT NOT NULL,                     -- 'subject' | 'context' | 'angle' | 'project' | 'other'
    description  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    created_by   INTEGER,                           -- chat_id who triggered creation; NULL for seeds
    is_seed      INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (created_by) REFERENCES users(chat_id)
);

-- Junction: each user_topic decomposes into 1+ atoms with include/exclude polarity.
-- e.g. 'stablecoins in tradfi' → include={stablecoins, tradfi}, exclude={defi}
CREATE TABLE IF NOT EXISTS topic_facets (
    chat_id   INTEGER NOT NULL,
    topic     TEXT NOT NULL,
    atom_id   TEXT NOT NULL,
    polarity  TEXT NOT NULL CHECK (polarity IN ('include', 'exclude')),
    PRIMARY KEY (chat_id, topic, atom_id, polarity),
    FOREIGN KEY (chat_id, topic) REFERENCES user_topics(chat_id, topic) ON DELETE CASCADE,
    FOREIGN KEY (atom_id) REFERENCES atoms(atom_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_topic_facets_atom ON topic_facets(atom_id);
"""


def now_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    current = row["version"] if row else 0
    if row is None:
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        return
    # Migration v1 → v2: add classified_at + classification_rationale columns
    # to user_topics for older DBs. atoms + topic_facets tables are created
    # idempotently above via CREATE TABLE IF NOT EXISTS.
    if current < 2:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(user_topics)")}
        if "classified_at" not in cols:
            conn.execute("ALTER TABLE user_topics ADD COLUMN classified_at TEXT")
        if "classification_rationale" not in cols:
            conn.execute("ALTER TABLE user_topics ADD COLUMN classification_rationale TEXT")
        conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))


# --- users -----------------------------------------------------------------

def upsert_user(conn: sqlite3.Connection, chat_id: int, username: str | None,
                first_name: str | None) -> bool:
    """Insert user if missing. Returns True if newly inserted, False if existed.

    Does NOT update existing rows here — that would cause every read-only
    command (e.g. /watchlist) to dirty the DB and force a git push. Use
    refresh_user_label() from a write-path command instead.
    """
    existing = conn.execute(
        "SELECT chat_id FROM users WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    if existing is None:
        now = now_utc()
        conn.execute(
            "INSERT INTO users (chat_id, username, first_name, joined_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (chat_id, username, first_name, now, now),
        )
        return True
    return False


def refresh_user_label(conn: sqlite3.Connection, chat_id: int,
                       username: str | None, first_name: str | None) -> bool:
    """Refresh the cached username/first_name for an existing user.

    Returns True if anything actually changed. Call from write-path command
    handlers (/add, /remove) where a git push is already happening — this
    keeps the display label fresh without adding new pushes.
    """
    row = conn.execute(
        "SELECT username, first_name FROM users WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    if row is None:
        return False
    if row["username"] == username and row["first_name"] == first_name:
        return False
    conn.execute(
        "UPDATE users SET username = ?, first_name = ?, last_seen_at = ? "
        "WHERE chat_id = ?",
        (username, first_name, now_utc(), chat_id),
    )
    return True


def all_users(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM users ORDER BY joined_at").fetchall()


# --- topics ----------------------------------------------------------------

def get_topics(conn: sqlite3.Connection, chat_id: int) -> list[str]:
    rows = conn.execute(
        "SELECT topic FROM user_topics WHERE chat_id = ? ORDER BY added_at",
        (chat_id,),
    ).fetchall()
    return [r["topic"] for r in rows]


def add_topic(conn: sqlite3.Connection, chat_id: int, topic: str) -> bool:
    """Add a topic. Returns True if inserted, False if it already existed.
    Also writes a topic_events row."""
    now = now_utc()
    try:
        conn.execute(
            "INSERT INTO user_topics (chat_id, topic, added_at) VALUES (?, ?, ?)",
            (chat_id, topic, now),
        )
    except sqlite3.IntegrityError:
        return False
    conn.execute(
        "INSERT INTO topic_events (chat_id, action, topic, occurred_at) "
        "VALUES (?, 'add', ?, ?)",
        (chat_id, topic, now),
    )
    return True


def remove_topic(conn: sqlite3.Connection, chat_id: int, topic: str) -> bool:
    """Remove a topic. Returns True if removed, False if it wasn't there."""
    now = now_utc()
    cur = conn.execute(
        "DELETE FROM user_topics WHERE chat_id = ? AND topic = ?",
        (chat_id, topic),
    )
    if cur.rowcount == 0:
        return False
    conn.execute(
        "INSERT INTO topic_events (chat_id, action, topic, occurred_at) "
        "VALUES (?, 'remove', ?, ?)",
        (chat_id, topic, now),
    )
    return True


def all_active_topics(conn: sqlite3.Connection) -> list[str]:
    """Return the union of every active topic across all users (for topics.md)."""
    rows = conn.execute(
        "SELECT DISTINCT topic FROM user_topics ORDER BY topic"
    ).fetchall()
    return [r["topic"] for r in rows]


def top_topics(conn: sqlite3.Connection, limit: int = 3) -> list[tuple[str, int]]:
    """Most-followed topics across all users."""
    rows = conn.execute(
        "SELECT topic, COUNT(*) AS c FROM user_topics "
        "GROUP BY topic ORDER BY c DESC, topic LIMIT ?",
        (limit,),
    ).fetchall()
    return [(r["topic"], r["c"]) for r in rows]


# --- per-user dedup --------------------------------------------------------

def has_seen_url(conn: sqlite3.Connection, chat_id: int, url: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM user_digested_urls WHERE chat_id = ? AND url = ?",
        (chat_id, url),
    ).fetchone() is not None


def record_seen_urls(conn: sqlite3.Connection, chat_id: int, urls: list[str]) -> None:
    if not urls:
        return
    now = now_utc()
    conn.executemany(
        "INSERT OR IGNORE INTO user_digested_urls (chat_id, url, surfaced_at) "
        "VALUES (?, ?, ?)",
        [(chat_id, u, now) for u in urls],
    )


def prune_old_digested(conn: sqlite3.Connection, days: int = DEDUP_KEEP_DAYS) -> int:
    cutoff = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    ).isoformat(timespec="seconds")
    cur = conn.execute(
        "DELETE FROM user_digested_urls WHERE surfaced_at < ?", (cutoff,)
    )
    return cur.rowcount


# --- per-user notifications ------------------------------------------------

def has_been_notified(conn: sqlite3.Connection, chat_id: int, filename: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM user_notifications WHERE chat_id = ? AND digest_filename = ?",
        (chat_id, filename),
    ).fetchone() is not None


def record_notified(conn: sqlite3.Connection, chat_id: int, filename: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO user_notifications "
        "(chat_id, digest_filename, sent_at) VALUES (?, ?, ?)",
        (chat_id, filename, now_utc()),
    )


# --- atoms + facets ------------------------------------------------------

def all_atoms(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT atom_id, kind, description, created_at, created_by, is_seed "
        "FROM atoms ORDER BY kind, atom_id"
    ).fetchall()


def get_atom(conn: sqlite3.Connection, atom_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT atom_id, kind, description, created_at, created_by, is_seed "
        "FROM atoms WHERE atom_id = ?", (atom_id,)
    ).fetchone()


def upsert_atom(conn: sqlite3.Connection, atom_id: str, kind: str,
                description: str, created_by: int | None = None,
                is_seed: bool = False) -> bool:
    """Insert atom if missing. Returns True if inserted, False if existed."""
    existing = conn.execute(
        "SELECT atom_id FROM atoms WHERE atom_id = ?", (atom_id,)
    ).fetchone()
    if existing is not None:
        return False
    conn.execute(
        "INSERT INTO atoms (atom_id, kind, description, created_at, created_by, is_seed) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (atom_id, kind, description, now_utc(), created_by, 1 if is_seed else 0),
    )
    return True


def get_facets(conn: sqlite3.Connection, chat_id: int, topic: str
              ) -> tuple[list[str], list[str]]:
    """Return (include_atoms, exclude_atoms) for a topic."""
    rows = conn.execute(
        "SELECT atom_id, polarity FROM topic_facets "
        "WHERE chat_id = ? AND topic = ? ORDER BY polarity, atom_id",
        (chat_id, topic),
    ).fetchall()
    inc = [r["atom_id"] for r in rows if r["polarity"] == "include"]
    exc = [r["atom_id"] for r in rows if r["polarity"] == "exclude"]
    return inc, exc


def set_facets(conn: sqlite3.Connection, chat_id: int, topic: str,
               include: list[str], exclude: list[str],
               rationale: str | None = None) -> None:
    """Replace all facet rows for a topic. Marks classified_at = now."""
    conn.execute(
        "DELETE FROM topic_facets WHERE chat_id = ? AND topic = ?",
        (chat_id, topic),
    )
    rows = (
        [(chat_id, topic, a, "include") for a in include]
        + [(chat_id, topic, a, "exclude") for a in exclude]
    )
    if rows:
        conn.executemany(
            "INSERT INTO topic_facets (chat_id, topic, atom_id, polarity) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    conn.execute(
        "UPDATE user_topics SET classified_at = ?, classification_rationale = ? "
        "WHERE chat_id = ? AND topic = ?",
        (now_utc(), rationale, chat_id, topic),
    )


def mark_unclassified(conn: sqlite3.Connection, chat_id: int, topic: str) -> bool:
    """Clear classification + facet rows so the next routine fire re-classifies.
    Returns True if the row existed."""
    cur = conn.execute(
        "DELETE FROM topic_facets WHERE chat_id = ? AND topic = ?",
        (chat_id, topic),
    )
    cur2 = conn.execute(
        "UPDATE user_topics SET classified_at = NULL, classification_rationale = NULL "
        "WHERE chat_id = ? AND topic = ?",
        (chat_id, topic),
    )
    return cur2.rowcount > 0


def unclassified_topics(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT chat_id, topic, added_at FROM user_topics "
        "WHERE classified_at IS NULL ORDER BY added_at"
    ).fetchall()


def atoms_created_since(conn: sqlite3.Connection, since_iso: str
                        ) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT atom_id, kind, description, created_at, created_by "
        "FROM atoms WHERE is_seed = 0 AND created_at >= ? "
        "ORDER BY created_at",
        (since_iso,),
    ).fetchall()


def topic_events_since(conn: sqlite3.Connection, since_iso: str
                       ) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, chat_id, action, topic, occurred_at "
        "FROM topic_events WHERE occurred_at >= ? ORDER BY occurred_at",
        (since_iso,),
    ).fetchall()


def merge_atoms(conn: sqlite3.Connection, keep_id: str, drop_id: str) -> int:
    """Repoint all topic_facets from drop_id to keep_id, then delete drop_id.
    Returns the number of facet rows updated. Idempotent."""
    if keep_id == drop_id:
        return 0
    if get_atom(conn, keep_id) is None or get_atom(conn, drop_id) is None:
        raise ValueError(f"unknown atom_id (keep={keep_id}, drop={drop_id})")
    cur = conn.execute(
        "UPDATE OR IGNORE topic_facets SET atom_id = ? WHERE atom_id = ?",
        (keep_id, drop_id),
    )
    n = cur.rowcount
    # Any rows that couldn't be updated due to (chat_id, topic, atom_id, polarity)
    # uniqueness collision are duplicates — drop them.
    conn.execute("DELETE FROM topic_facets WHERE atom_id = ?", (drop_id,))
    conn.execute("DELETE FROM atoms WHERE atom_id = ?", (drop_id,))
    return n
