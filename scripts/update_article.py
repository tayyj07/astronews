#!/usr/bin/env python3
"""Update a single article's cache entry with derived fields.

Used by the cloud routine after generating a summary or classifying user
topics. The entry must already exist (the local scraper creates it when
the article is first detected).

    python3 scripts/update_article.py \\
      --url "https://..." \\
      --summary "1-2 sentence summary text" \\
      --user-topics "topic A,topic B"
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import pathlib
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--summary", default=None)
    ap.add_argument(
        "--user-topics",
        default=None,
        help="comma-separated list of topic strings matching topics.md",
    )
    args = ap.parse_args()

    digest = hashlib.sha256(args.url.encode("utf-8")).hexdigest()[:16]
    path = pathlib.Path("state/articles") / f"{digest}.json"
    if not path.exists():
        print(f"No cache entry at {path} for url {args.url}", file=sys.stderr)
        return 1

    data = json.loads(path.read_text())
    if args.summary is not None:
        data["summary"] = args.summary.strip()
    if args.user_topics is not None:
        data["user_topics"] = [
            t.strip() for t in args.user_topics.split(",") if t.strip()
        ]
    data["enriched_at"] = (
        datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    )

    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(f"updated {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
