#!/usr/bin/env python3
"""AstroNews Telegram notifier.

Watches the repo for new digests and forwards each unsent one to the
configured Telegram chat. Tracks sent digests in `state/notified.json`,
which is gitignored — it's per-runtime local state, not shared.

Requires `~/.config/astronews/credentials.env` with:
    TELEGRAM_BOT_TOKEN=...
    TELEGRAM_CHAT_ID=...

Usage:
    python3 scripts/notify.py            # one-shot run
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

REPO_ROOT = Path(__file__).resolve().parent.parent
DIGESTS_DIR = REPO_ROOT / "digests"
STATE_DIR = REPO_ROOT / "state"
NOTIFIED_FILE = STATE_DIR / "notified.json"
CREDENTIALS_FILE = Path(os.path.expanduser("~/.config/astronews/credentials.env"))

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_LIMIT = 4096  # bytes per message
SAFE_CHUNK = 3800      # leave headroom for a "(part N/M)" suffix


def load_credentials() -> dict:
    if not CREDENTIALS_FILE.exists():
        sys.exit(f"missing credentials at {CREDENTIALS_FILE}")
    creds = {}
    for line in CREDENTIALS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip().strip('"').strip("'")
    for required in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if not creds.get(required):
            sys.exit(f"missing {required} in {CREDENTIALS_FILE}")
    return creds


# --- markdown → Telegram HTML ---------------------------------------------

_BULLET_SOURCE_RE = re.compile(
    r'^(\s*-)\s+(.+?)\s*\(\[([^\]]+)\]\(([^)]+)\)\)\s*$',
    re.MULTILINE,
)


def reformat_source_first(md: str) -> str:
    """Move the trailing ([Source](url)) on each bullet to the front.

    Input:   `- BIP-361 ... at risk. ([Coindesk](https://...))`
    Output:  `- ([Coindesk](https://...)) BIP-361 ... at risk.`

    After HTML conversion the source becomes a parenthesized clickable link
    at the start of the bullet, matching the user's preferred layout.
    """
    def repl(m: re.Match) -> str:
        prefix, text, source, url = m.group(1), m.group(2).rstrip(), m.group(3), m.group(4)
        return f'{prefix} ([{source}]({url})) {text}'
    return _BULLET_SOURCE_RE.sub(repl, md)


def md_to_telegram_html(md: str) -> str:
    """Convert the digest markdown to Telegram-flavored HTML.

    Telegram HTML supports: <b>, <i>, <u>, <s>, <code>, <pre>, <a>. No headings.
    We map H1/H2 to <b>, bullets to "• ", and links to <a>. Bullets get
    their source link moved to the start (parenthesized).
    """
    md = reformat_source_first(md)
    text = html.escape(md, quote=False)
    # Headings → bold (H1/H2/H3 all become <b>)
    text = re.sub(r"^#{1,3}\s+(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)
    # **bold**
    text = re.sub(r"\*\*([^*\n]+?)\*\*", r"<b>\1</b>", text)
    # _italic_ — only when underscores aren't adjacent to word chars (avoids URL fragments)
    text = re.sub(r"(?<!\w)_([^_\n]+)_(?!\w)", r"<i>\1</i>", text)
    # Bullets
    text = re.sub(r"^- ", "• ", text, flags=re.MULTILINE)
    # Markdown links [text](url) → <a href="url">text</a>
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


def _is_heading(line: str) -> bool:
    s = line.strip()
    return s.startswith("<b>") and s.endswith("</b>")


def split_for_telegram(text: str, limit: int = SAFE_CHUNK) -> list[str]:
    """Split text into messages, keeping each topic-section (H2 and its
    bullets) in one message whenever possible.

    Algorithm:
    1. Parse into blocks. A block = a heading line followed by its body
       (lines up to the next heading). Blocks are kept atomic during packing.
    2. Greedy-pack blocks into messages: each block starts in a new message
       if it doesn't fit alongside what's already accumulated.
    3. If a single block exceeds the per-message limit, split its body across
       messages but reprise the heading at the start of each chunk so the
       reader always knows which topic they're inside.
    """
    if len(text.encode("utf-8")) <= limit:
        return [text]

    # --- step 1: collect blocks
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

    # --- step 2: pack blocks
    messages: list[str] = []
    msg: list[str] = []
    msg_size = 0
    sep = "\n\n"
    sep_bytes = len(sep.encode("utf-8"))

    def block_text(b: list[str]) -> str:
        return "\n".join(b).rstrip()

    def flush_msg() -> None:
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
            # Oversized block: flush current msg, then split this block with
            # heading repetition.
            flush_msg()
            heading = block[0] if block and _is_heading(block[0]) else ""
            body = block[1:] if heading else block
            heading_size = len(heading.encode("utf-8")) + 1 if heading else 0

            sub: list[str] = [heading] if heading else []
            sub_size = heading_size

            for line in body:
                line_size = len(line.encode("utf-8")) + 1
                projected = sub_size + line_size
                # Only flush if we already have body content (not just heading)
                if projected > limit and len(sub) > (1 if heading else 0):
                    messages.append("\n".join(sub))
                    sub = [heading] if heading else []
                    sub_size = heading_size
                sub.append(line)
                sub_size += line_size
            if sub and (len(sub) > (1 if heading else 0) or not heading):
                messages.append("\n".join(sub))
        elif msg_size + b_size > limit:
            flush_msg()
            msg = [text_b]
            msg_size = len(text_b.encode("utf-8"))
        else:
            msg.append(text_b)
            msg_size += b_size

    flush_msg()
    return messages


def send_message(token: str, chat_id: str, text: str) -> dict:
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


# --- state ---------------------------------------------------------------

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


def git(*args: str) -> str:
    res = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return res.stdout.strip()


# --- main ----------------------------------------------------------------

def main() -> int:
    creds = load_credentials()
    token = creds["TELEGRAM_BOT_TOKEN"]
    chat_id = creds["TELEGRAM_CHAT_ID"]

    # Pull latest digests from the repo so we see new cloud-routine pushes.
    try:
        git("pull", "--rebase", "--quiet", "origin", "main")
    except subprocess.CalledProcessError as e:
        print(f"git pull failed: {e.stderr.strip() if e.stderr else e}")
        return 1

    notified = set(load_json(NOTIFIED_FILE, []))
    digests = sorted(DIGESTS_DIR.glob("*.md"))
    pending = [d for d in digests if d.name not in notified]

    if not pending:
        print(f"no new digests ({len(digests)} total, all notified)")
        return 0

    print(f"{len(pending)} pending digest(s) to send")
    for digest_path in pending:
        md = digest_path.read_text()
        html_body = md_to_telegram_html(md)
        chunks = split_for_telegram(html_body)
        print(f"  {digest_path.name}: {len(chunks)} message(s)")
        for i, chunk in enumerate(chunks, 1):
            suffix = f"\n\n<i>(part {i}/{len(chunks)})</i>" if len(chunks) > 1 else ""
            try:
                resp = send_message(token, chat_id, chunk + suffix)
            except Exception as e:  # noqa: BLE001
                print(f"    send failed: {type(e).__name__}: {e}")
                return 1
            if not resp.get("ok"):
                print(f"    Telegram returned error: {resp}")
                return 1
        notified.add(digest_path.name)

    save_json(NOTIFIED_FILE, sorted(notified))
    print(f"done; notified.json now has {len(notified)} entries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
