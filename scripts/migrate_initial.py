#!/usr/bin/env python3
"""One-shot migration to seed the SQLite DB with state from before the
multi-user rollout.

What it does:
    1. Initialises the schema.
    2. Inserts your existing chat_id (655916874) as user `tayyj07` if missing.
    3. Reads non-comment lines from `topics.md` and treats them as your
       initial topic subscriptions (subject to the per-user cap).
    4. Imports `state/digested_urls.json` (if any) into your `user_digested_urls`
       so the new dedup logic doesn't re-surface URLs you've already seen.

Idempotent: running it twice is a no-op (everything goes through INSERT OR
IGNORE / existence checks).

Usage:
    python3 scripts/migrate_initial.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as dbm  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
TOPICS_FILE = REPO_ROOT / "topics.md"
LEGACY_DIGESTED = REPO_ROOT / "state" / "digested_urls.json"

OWNER_CHAT_ID = 655916874
OWNER_USERNAME = "tayyj07"
OWNER_FIRST_NAME = "YJ"


def parse_topics(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def main() -> int:
    conn = dbm.connect()
    dbm.init_schema(conn)

    inserted = dbm.upsert_user(conn, OWNER_CHAT_ID, OWNER_USERNAME, OWNER_FIRST_NAME)
    print(f"user {OWNER_CHAT_ID}: {'inserted' if inserted else 'already present'}")

    # Topics
    if TOPICS_FILE.exists():
        topics = parse_topics(TOPICS_FILE.read_text())
        existing = set(dbm.get_topics(conn, OWNER_CHAT_ID))
        added = 0
        for t in topics:
            if t in existing:
                continue
            if dbm.add_topic(conn, OWNER_CHAT_ID, t):
                added += 1
        print(f"topics: {added} added (existing {len(existing)}, file lists {len(topics)})")
    else:
        print("topics.md missing — skipping")

    # digested URLs from the legacy global file
    if LEGACY_DIGESTED.exists():
        try:
            data = json.loads(LEGACY_DIGESTED.read_text())
            urls = list(data.keys()) if isinstance(data, dict) else list(data)
            dbm.record_seen_urls(conn, OWNER_CHAT_ID, urls)
            print(f"digested_urls: imported {len(urls)} URLs into user_digested_urls")
        except Exception as e:  # noqa: BLE001
            print(f"could not read {LEGACY_DIGESTED}: {e}", file=sys.stderr)

    n_topics = len(dbm.get_topics(conn, OWNER_CHAT_ID))
    print(f"\nfinal: {n_topics} topics for user {OWNER_CHAT_ID}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
