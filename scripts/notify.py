#!/usr/bin/env python3
"""AstroNews Telegram notifier — multi-user.

For each registered user (rows in `users` table):
    1. Find digest files in `digests/` not yet sent to this user.
    2. Parse each digest into H2 sections.
    3. Keep only sections matching the user's topics (case-insensitive).
    4. Within each kept section, drop bullets whose URL is in this user's
       `user_digested_urls` table (per-user dedup).
    5. If anything substantive remains, send to the user's chat_id (with
       source-first formatting and topic-atomic chunking).
    6. Mark the digest as notified for the user; record surfaced URLs.

Reads `~/.config/astronews/credentials.env` for `TELEGRAM_BOT_TOKEN`.
Each user's chat_id comes from the DB (the `users` table), not the env.
"""

from __future__ import annotations

import datetime
import html
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as dbm  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DIGESTS_DIR = REPO_ROOT / "digests"
INBOX_FILE = REPO_ROOT / "state" / "inbox.json"
CREDENTIALS_FILE = Path(os.path.expanduser("~/.config/astronews/credentials.env"))

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_LIMIT = 4096
SAFE_CHUNK = 3800


# --- credentials ----------------------------------------------------------

def load_token() -> str:
    if not CREDENTIALS_FILE.exists():
        sys.exit(f"missing credentials at {CREDENTIALS_FILE}")
    for raw in CREDENTIALS_FILE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == "TELEGRAM_BOT_TOKEN":
            return v.strip().strip('"').strip("'")
    sys.exit("TELEGRAM_BOT_TOKEN missing from credentials")


# --- digest parsing -------------------------------------------------------

# Pattern: matches markdown links inside a parenthesised "source" group:
# `([Coindesk](https://...))`
_BULLET_LINK_RE = re.compile(r'\(\[([^\]]+)\]\(([^)]+)\)\)')


def parse_digest(md: str) -> tuple[str, list[tuple[str, list[str]]]]:
    """Split digest into (h1_text, [(h2_heading, body_lines), ...])."""
    h1 = ""
    sections: list[tuple[str, list[str]]] = []
    current_h2: str | None = None
    current_body: list[str] = []
    for line in md.splitlines():
        if line.startswith("# ") and not h1:
            h1 = line[2:].strip()
        elif line.startswith("## "):
            if current_h2 is not None:
                sections.append((current_h2, current_body))
            current_h2 = line[3:].strip()
            current_body = []
        else:
            if current_h2 is not None:
                current_body.append(line)
    if current_h2 is not None:
        sections.append((current_h2, current_body))
    return h1, sections


def bullet_urls(line: str) -> list[str]:
    return [m.group(2) for m in _BULLET_LINK_RE.finditer(line)]


def filter_bullets(body: list[str], seen_urls: set[str]) -> list[str]:
    """Drop bullets that contain a URL already in seen_urls. Keep non-bullet
    lines (blank lines, italic placeholders, etc.) untouched."""
    out: list[str] = []
    for line in body:
        if line.lstrip().startswith("- "):
            urls = bullet_urls(line)
            if urls and any(u in seen_urls for u in urls):
                continue
        out.append(line)
    return out


def has_substantive_bullet(body: list[str]) -> bool:
    return any(line.lstrip().startswith("- ") for line in body)


def _domain_to_outlet(url: str) -> str:
    """Derive a human-friendly outlet label from a URL's hostname."""
    try:
        host = urllib.parse.urlparse(url).hostname or ""
    except Exception:  # noqa: BLE001
        host = ""
    host = host.removeprefix("www.")
    parts = host.split(".")
    name = parts[0] if parts else "source"
    # Title-case but keep common acronyms uppercase
    return name.replace("-", " ").title() or "Source"


def _format_inbox_bullet(item: dict) -> str:
    """Render an inbox item as a markdown bullet with source-first formatting."""
    url = item["url"]
    title = (item.get("title") or "").strip() or "(no title)"
    date = item.get("date")
    outlet = _domain_to_outlet(url)
    date_prefix = f"[{date}] " if date else ""
    return f"- {date_prefix}{title}. ([{outlet}]({url}))"


def build_user_view(h1: str, sections: list[tuple[str, list[str]]],
                    user_topics: list[str], seen_urls: set[str],
                    user_sources_map: dict[str, str | None],
                    inbox: list[dict],
                    ) -> tuple[str | None, list[str], list[str]]:
    """Build a per-user filtered digest. Returns (md, surfaced_urls, inbox_consumed_urls).

    - `user_topics`: the user's topic strings.
    - `user_sources_map`: {source_url: route_topic_or_None} — the user's source subscriptions.
    - `inbox`: list of inbox items the scraper queued globally; we filter to this user's sources.

    Returns (None, [], []) if there's no substantive content for the user.
    """
    topic_lookup = {t.lower(): t for t in user_topics}
    surfaced: list[str] = []
    inbox_consumed: list[str] = []
    any_content = False

    # Step 1: collect bullets from the global digest's H2 sections that match
    # this user's topics.
    topic_blocks: dict[str, list[str]] = {}
    for heading, body in sections:
        if heading.lower() not in topic_lookup:
            continue
        filtered = filter_bullets(body, seen_urls)
        while filtered and not filtered[0].strip():
            filtered.pop(0)
        while filtered and not filtered[-1].strip():
            filtered.pop()
        canonical = topic_lookup[heading.lower()]
        topic_blocks.setdefault(canonical, []).extend(filtered)

    # Step 2: pull inbox items the user follows; route by their user_sources topic.
    watched_sources_bullets: list[str] = []
    for item in inbox:
        src = item.get("source_url")
        if src not in user_sources_map:
            continue  # user doesn't follow this source
        if item["url"] in seen_urls:
            continue  # already delivered to this user
        bullet = _format_inbox_bullet(item)
        route = user_sources_map[src]
        if route and route.lower() in topic_lookup:
            topic_blocks.setdefault(topic_lookup[route.lower()], []).append(bullet)
        else:
            watched_sources_bullets.append(bullet)
        surfaced.append(item["url"])
        inbox_consumed.append(item["url"])

    # Step 3: assemble the user's view.
    output: list[str] = [f"# {h1}"] if h1 else []
    for topic in user_topics:
        block = topic_blocks.get(topic, [])
        output.append("")
        output.append(f"## {topic}")
        if has_substantive_bullet(block):
            any_content = True
            output.extend(block)
            for line in block:
                surfaced.extend(bullet_urls(line))
        else:
            output.append("_No new material in the last 6 hours._")

    if watched_sources_bullets:
        any_content = True
        output.append("")
        output.append("## Watched sources")
        output.extend(watched_sources_bullets)

    if not any_content:
        return None, [], []
    return ("\n".join(output).rstrip() + "\n",
            list(dict.fromkeys(surfaced)),
            list(dict.fromkeys(inbox_consumed)))


# --- markdown → Telegram HTML --------------------------------------------

_BULLET_SOURCE_RE = re.compile(
    r'^(\s*-)\s+(.+?)\s*\(\[([^\]]+)\]\(([^)]+)\)\)\s*$',
    re.MULTILINE,
)


def reformat_source_first(md: str) -> str:
    def repl(m: re.Match) -> str:
        prefix, text, source, url = m.group(1), m.group(2).rstrip(), m.group(3), m.group(4)
        return f'{prefix} ([{source}]({url})) {text}'
    return _BULLET_SOURCE_RE.sub(repl, md)


def md_to_telegram_html(md: str) -> str:
    md = reformat_source_first(md)
    text = html.escape(md, quote=False)
    text = re.sub(r"^#{1,3}\s+(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*([^*\n]+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\w)_([^_\n]+)_(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"^- ", "• ", text, flags=re.MULTILINE)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


def _is_heading(line: str) -> bool:
    s = line.strip()
    return s.startswith("<b>") and s.endswith("</b>")


def split_for_telegram(text: str, limit: int = SAFE_CHUNK) -> list[str]:
    """Topic-atomic split. Each H2 section stays in one message when possible;
    oversized sections split with the heading reprised on each chunk."""
    if len(text.encode("utf-8")) <= limit:
        return [text]
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in text.split("\n"):
        if _is_heading(line):
            if current:
                blocks.append(current)
            current = [line]
        else:
            if not current:
                current = []
            current.append(line)
    if current:
        blocks.append(current)

    messages: list[str] = []
    msg: list[str] = []
    msg_size = 0
    sep = "\n\n"
    sep_bytes = len(sep.encode("utf-8"))

    def block_text(b: list[str]) -> str:
        return "\n".join(b).rstrip()

    def flush() -> None:
        nonlocal msg, msg_size
        if msg:
            messages.append(sep.join(msg))
            msg = []
            msg_size = 0

    for block in blocks:
        text_b = block_text(block)
        if not text_b:
            continue
        b_size = len(text_b.encode("utf-8")) + (sep_bytes if msg else 0)
        if b_size > limit:
            flush()
            heading = block[0] if block and _is_heading(block[0]) else ""
            body = block[1:] if heading else block
            heading_size = len(heading.encode("utf-8")) + 1 if heading else 0
            sub: list[str] = [heading] if heading else []
            sub_size = heading_size
            for line in body:
                line_size = len(line.encode("utf-8")) + 1
                if sub_size + line_size > limit and len(sub) > (1 if heading else 0):
                    messages.append("\n".join(sub))
                    sub = [heading] if heading else []
                    sub_size = heading_size
                sub.append(line)
                sub_size += line_size
            if sub and (len(sub) > (1 if heading else 0) or not heading):
                messages.append("\n".join(sub))
        elif msg_size + b_size > limit:
            flush()
            msg = [text_b]
            msg_size = len(text_b.encode("utf-8"))
        else:
            msg.append(text_b)
            msg_size += b_size
    flush()
    return messages


# --- send -----------------------------------------------------------------

def send_message(token: str, chat_id: int, text: str) -> dict:
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


# --- git ------------------------------------------------------------------

def git(*args: str) -> tuple[int, str, str]:
    res = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True,
    )
    return res.returncode, res.stdout.strip(), res.stderr.strip()


def _load_inbox() -> list[dict]:
    if not INBOX_FILE.exists():
        return []
    try:
        data = json.loads(INBOX_FILE.read_text())
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _drain_inbox(conn, original_inbox: list[dict]) -> None:
    """Remove inbox items whose URL has been delivered to every current
    follower of the source. Anything still pending stays in the inbox.
    Re-reads the inbox from disk so concurrent scraper appends aren't lost.
    """
    current = _load_inbox()
    by_url = {item["url"]: item for item in current}
    surviving: list[dict] = []
    for url, item in by_url.items():
        src = item.get("source_url")
        if not src:
            surviving.append(item)
            continue
        followers = dbm.followers_of_source(conn, src)
        if not followers:
            continue  # no one cares anymore — drop it
        all_seen = all(
            dbm.has_seen_url(conn, f["chat_id"], url) for f in followers
        )
        if not all_seen:
            surviving.append(item)
    if len(surviving) != len(current):
        INBOX_FILE.parent.mkdir(parents=True, exist_ok=True)
        INBOX_FILE.write_text(json.dumps(surviving, indent=2, sort_keys=True) + "\n")
        print(f"inbox drained: kept {len(surviving)} of {len(current)}")


# --- main -----------------------------------------------------------------

def main() -> int:
    token = load_token()

    code, _, err = git("pull", "--rebase", "--quiet", "origin", "main")
    if code != 0:
        print(f"git pull failed: {err}", file=sys.stderr)
        return 1

    conn = dbm.connect()
    dbm.init_schema(conn)

    users = dbm.all_users(conn)
    if not users:
        print("no registered users — nothing to do")
        conn.close()
        return 0

    digest_files = sorted(DIGESTS_DIR.glob("*.md"))
    if not digest_files:
        print("no digest files yet")
        conn.close()
        return 0

    # Parse each digest once, reuse across users.
    parsed_digests: list[tuple[Path, str, list[tuple[str, list[str]]]]] = []
    for path in digest_files:
        h1, sections = parse_digest(path.read_text())
        parsed_digests.append((path, h1, sections))

    inbox = _load_inbox()

    sent_count = 0
    for user in users:
        chat_id = user["chat_id"]
        topics = dbm.get_topics(conn, chat_id)
        user_src_rows = dbm.get_user_sources(conn, chat_id)
        user_sources_map = {r["source_url"]: r["topic"] for r in user_src_rows}
        if not topics and not user_sources_map:
            continue
        for path, h1, sections in parsed_digests:
            if dbm.has_been_notified(conn, chat_id, path.name):
                continue
            seen = {
                row["url"] for row in conn.execute(
                    "SELECT url FROM user_digested_urls WHERE chat_id = ?",
                    (chat_id,),
                )
            }
            md_view, surfaced, _ = build_user_view(
                h1, sections, topics, seen, user_sources_map, inbox,
            )
            if md_view is None:
                dbm.record_notified(conn, chat_id, path.name)
                continue
            html_body = md_to_telegram_html(md_view)
            chunks = split_for_telegram(html_body)
            print(f"  user={chat_id} digest={path.name}: {len(chunks)} message(s)")
            ok = True
            for i, chunk in enumerate(chunks, 1):
                suffix = f"\n\n<i>(part {i}/{len(chunks)})</i>" if len(chunks) > 1 else ""
                try:
                    resp = send_message(token, chat_id, chunk + suffix)
                except Exception as e:  # noqa: BLE001
                    print(f"    send failed: {type(e).__name__}: {e}", file=sys.stderr)
                    ok = False
                    break
                if not resp.get("ok"):
                    print(f"    Telegram error: {resp}", file=sys.stderr)
                    ok = False
                    break
            if ok:
                dbm.record_notified(conn, chat_id, path.name)
                dbm.record_seen_urls(conn, chat_id, surfaced)
                sent_count += 1

    # Drain inbox: keep items only while at least one follower hasn't seen them.
    _drain_inbox(conn, inbox)

    pruned = dbm.prune_old_digested(conn)
    if pruned:
        print(f"pruned {pruned} old user_digested_urls rows")

    conn.close()

    # Push DB updates so origin reflects per-user state.
    code, _, _ = git("diff", "--quiet", "state/astronews.db")
    db_changed = (code != 0)
    if db_changed:
        git("add", "state/astronews.db")
        git(
            "-c", "user.name=AstroNews Notifier",
            "-c", "user.email=astronews-notifier@noreply.local",
            "commit", "-m", f"notifier: sent {sent_count} digest(s)",
        )
        for _ in range(2):
            code, _, err = git("push", "origin", "main")
            if code == 0:
                break
            git("pull", "--rebase", "--quiet", "origin", "main")
        else:
            print("push failed after retry", file=sys.stderr)
            return 1
    print(f"done; {sent_count} digest(s) sent across {len(users)} user(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
