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

SCHEMA_VERSION = 1
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
    chat_id   INTEGER NOT NULL,
    topic     TEXT NOT NULL,
    added_at  TEXT NOT NULL,
    PRIMARY KEY (chat_id, topic),
    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_user_topics_topic ON user_topics(topic);

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
    if row is None:
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))


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
