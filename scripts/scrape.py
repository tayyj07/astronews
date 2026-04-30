#!/usr/bin/env python3
"""AstroNews scraper.

Reads sources.md, fetches each watched URL, extracts posts via pluggable
strategies, computes deltas vs state/seen.json, writes per-article metadata
into state/articles/<sha>.json (the knowledge base), and appends new-post
pointers into state/inbox.json for the cloud routine to drain.

Designed to be portable: state lives in JSON files inside the repo, the only
runtime dependency is Python's stdlib, and extraction strategies are stand-
alone functions you can extend by adding to the EXTRACTORS list.

Usage:
    python3 scripts/scrape.py            # one-shot run
"""

from __future__ import annotations

import datetime
import hashlib
import html as html_lib
import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCES_FILE = REPO_ROOT / "sources.md"
STATE_DIR = REPO_ROOT / "state"
SEEN_FILE = STATE_DIR / "seen.json"
INBOX_FILE = STATE_DIR / "inbox.json"
ARTICLES_DIR = STATE_DIR / "articles"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Safari/605.1.15"
)

# Post: {"url": str, "title": str, "date": str|None,
#        "raw_excerpt": str|None, "publisher_tags": list[str]}
Post = dict


# --- sources.md parsing ---------------------------------------------------

def parse_sources(text: str) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        for sep in ("→", "->"):
            if sep in line:
                url_part, topic = line.split(sep, 1)
                out.append((url_part.strip(), topic.strip()))
                break
        else:
            out.append((line, None))
    return out


# --- fetching -------------------------------------------------------------

def fetch(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        charset = r.headers.get_content_charset() or "utf-8"
        return r.read().decode(charset, errors="replace")


def absolutize(source_url: str, href: str) -> str:
    return urllib.parse.urljoin(source_url, href)


# --- extraction strategies ------------------------------------------------

def _dedupe_preserve_order(items):
    return list(dict.fromkeys(i for i in items if i))


def extract_embedded_json(html_text: str, source_url: str) -> list[Post]:
    """Sites that embed posts as JSON objects in HTML.

    Anchors on the `"objectID":` marker, then uses json.JSONDecoder.raw_decode
    to parse the entire object — captures title, date, permalink, excerpt, and
    publisher tags in one pass. Confirmed working on a16zcrypto.com.
    """
    decoded = html_lib.unescape(html_text)
    decoder = json.JSONDecoder()
    seen_urls: set[str] = set()
    posts: list[Post] = []
    for match in re.finditer(r'\{"objectID":', decoded):
        try:
            obj, _ = decoder.raw_decode(decoded[match.start():])
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        title = obj.get("post_title")
        path = obj.get("permalink")
        if not (title and path):
            continue
        url = absolutize(source_url, path)
        if url in seen_urls:
            continue
        seen_urls.add(url)

        ts = obj.get("post_date")
        try:
            date = (
                datetime.datetime
                .fromtimestamp(int(ts), tz=datetime.timezone.utc)
                .date()
                .isoformat()
            ) if ts else None
        except Exception:
            date = None

        tax = obj.get("taxonomies") or {}
        publisher_tags = _dedupe_preserve_order(
            list(tax.get("post_tag") or []) + list(tax.get("category") or [])
        )

        excerpt = (obj.get("post_excerpt") or obj.get("post_description") or "").strip() or None

        posts.append({
            "url": url,
            "title": title,
            "date": date,
            "raw_excerpt": excerpt,
            "publisher_tags": publisher_tags,
        })
    return posts


_TAG_RE = re.compile(r"<[^>]+>")
_POST_PATH_HINT = re.compile(
    r"/(posts?|article[s]?|blog|news|p)/[^/?#]+/?$",
    re.IGNORECASE,
)


def extract_html_anchors(html_text: str, source_url: str) -> list[Post]:
    """Generic fallback: anchor tags whose href looks like a post path."""
    pattern = re.compile(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    seen_urls: set[str] = set()
    posts: list[Post] = []
    for href, inner in pattern.findall(html_text):
        if not _POST_PATH_HINT.search(href.split("#")[0].split("?")[0]):
            continue
        title = _TAG_RE.sub("", inner).strip()
        title = html_lib.unescape(re.sub(r"\s+", " ", title))
        if not title or len(title) < 12:
            continue
        url = absolutize(source_url, href)
        if url in seen_urls:
            continue
        seen_urls.add(url)
        posts.append({
            "url": url,
            "title": title,
            "date": None,
            "raw_excerpt": None,
            "publisher_tags": [],
        })
    return posts


EXTRACTORS: list[Callable[[str, str], list[Post]]] = [
    extract_embedded_json,
    extract_html_anchors,
]


def extract(html_text: str, source_url: str) -> tuple[str | None, list[Post]]:
    for fn in EXTRACTORS:
        try:
            posts = fn(html_text, source_url)
        except Exception as e:  # noqa: BLE001
            print(f"  extractor {fn.__name__} raised {e!r}")
            continue
        if posts:
            return fn.__name__, posts
    return None, []


# --- enrichment -----------------------------------------------------------

_OG_DESC_RE = re.compile(
    r'<meta\s+[^>]*?(?:property|name)=["\']og:description["\']\s+[^>]*?content=["\']([^"\']*)["\']',
    re.IGNORECASE,
)


def enrich(post: Post) -> Post:
    """Augment a post in-place with raw_excerpt from og:description if missing.

    Costs one extra HTTP fetch per post that lacks an excerpt — only called for
    NEW posts (not seed-run posts), so cost is bounded by the publish rate of
    the source.
    """
    if post.get("raw_excerpt"):
        return post
    try:
        html_text = fetch(post["url"])
    except Exception as e:  # noqa: BLE001
        print(f"    enrich fetch failed for {post['url']}: {type(e).__name__}: {e}")
        return post
    m = _OG_DESC_RE.search(html_text)
    if m:
        post["raw_excerpt"] = html_lib.unescape(m.group(1)).strip() or None
    return post


# --- article cache --------------------------------------------------------

def article_cache_path(url: str) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return ARTICLES_DIR / f"{digest}.json"


def upsert_article(post: Post, source_url: str) -> Path:
    """Write the post into the article cache, preserving any derived fields
    (summary, user_topics, etc.) added by other consumers."""
    path = article_cache_path(post["url"])
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_json(path, {})
    merged = {
        **existing,
        "url": post["url"],
        "source_url": source_url,
        "title": post["title"],
        "date": post.get("date"),
        "raw_excerpt": post.get("raw_excerpt") or existing.get("raw_excerpt"),
        "publisher_tags": post.get("publisher_tags") or existing.get("publisher_tags") or [],
        "fetched_at": existing.get("fetched_at") or datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    }
    save_json(path, merged)
    return path


# --- JSON state ----------------------------------------------------------

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


# --- git ------------------------------------------------------------------

def git(*args: str) -> str:
    res = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return res.stdout.strip()


# --- main -----------------------------------------------------------------

def main() -> int:
    print(f"AstroNews scraper @ {datetime.datetime.now(datetime.timezone.utc).isoformat()}")

    git("pull", "--rebase", "--quiet", "origin", "main")

    sources = parse_sources(SOURCES_FILE.read_text())
    if not sources:
        print("No sources to scrape.")
        return 0

    seen: dict[str, list[str]] = load_json(SEEN_FILE, {})
    inbox: list[dict] = load_json(INBOX_FILE, [])
    if not isinstance(inbox, list):
        inbox = []

    summary: list[str] = []
    seeded = 0
    new_total = 0

    for source_url, topic in sources:
        print(f"\n[{source_url}]  topic={topic!r}")
        try:
            html_text = fetch(source_url)
        except Exception as e:  # noqa: BLE001
            print(f"  FETCH FAILED: {type(e).__name__}: {e}")
            summary.append(f"{source_url}: fetch failed ({type(e).__name__})")
            continue

        extractor, posts = extract(html_text, source_url)
        if not posts:
            print("  No extractor yielded posts.")
            summary.append(f"{source_url}: 0 posts extracted")
            continue
        print(f"  extractor={extractor}  posts={len(posts)}")

        # Always upsert each post into the article cache — idempotent, gives us
        # a knowledge base that grows monotonically across runs.
        for p in posts:
            upsert_article(p, source_url)

        previous = set(seen.get(source_url, []))
        is_seed = source_url not in seen or not previous
        all_urls = {p["url"] for p in posts}

        if is_seed:
            print(f"  SEED — recording {len(all_urls)} URL(s) as seen, surfacing nothing.")
            seen[source_url] = sorted(all_urls)
            seeded += 1
            summary.append(f"{source_url}: seeded ({len(all_urls)} posts)")
            continue

        new_posts = [p for p in posts if p["url"] not in previous]
        if not new_posts:
            print("  No new posts.")
            seen[source_url] = sorted(previous | all_urls)
            summary.append(f"{source_url}: no new posts")
            continue

        print(f"  {len(new_posts)} new — enriching:")
        for p in new_posts:
            if not p.get("raw_excerpt"):
                enrich(p)
            upsert_article(p, source_url)  # re-upsert with enriched data
            print(f"    - {p['date'] or '????-??-??'}  {p['title'][:80]}")
            inbox.append({
                "source_url": source_url,
                "topic": topic,
                "url": p["url"],
                "title": p["title"],
                "date": p["date"],
                "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            })
        seen[source_url] = sorted(previous | all_urls)
        new_total += len(new_posts)
        summary.append(f"{source_url}: {len(new_posts)} new")

    save_json(SEEN_FILE, seen)
    save_json(INBOX_FILE, inbox)

    status = git("status", "--porcelain", "state/")
    if not status:
        print("\nNo state changes — skipping commit.")
        return 0

    git("add", "state/")
    parts = [f"{new_total} new"]
    if seeded:
        parts.append(f"{seeded} seeded")
    msg = f"scraper: {', '.join(parts)} across {len(sources)} source(s)"
    git(
        "-c", "user.name=AstroNews Scraper",
        "-c", "user.email=astronews-scraper@users.noreply.github.com",
        "commit", "-m", msg,
    )
    git("push", "origin", "main")
    print(f"\nPushed: {msg}")
    print("Summary:")
    for line in summary:
        print(f"  - {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
