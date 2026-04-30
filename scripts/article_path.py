#!/usr/bin/env python3
"""Print the article cache path for a URL. Used by the cloud routine.

    python3 scripts/article_path.py <url>     # → state/articles/<sha>.json
"""
import hashlib
import sys

if len(sys.argv) != 2:
    print("usage: article_path.py <url>", file=sys.stderr)
    sys.exit(2)

digest = hashlib.sha256(sys.argv[1].encode("utf-8")).hexdigest()[:16]
print(f"state/articles/{digest}.json")
