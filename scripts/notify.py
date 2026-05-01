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

def md_to_telegram_html(md: str) -> str:
    """Convert the digest markdown to Telegram-flavored HTML.

    Telegram HTML supports: <b>, <i>, <u>, <s>, <code>, <pre>, <a>. No headings.
    We map H1/H2 to <b>, bullets to "• ", and links to <a>.
    """
    text = html.escape(md, quote=False)
    # Headings → bold + newline
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


def split_for_telegram(text: str, limit: int = SAFE_CHUNK) -> list[str]:
    """Split text into chunks that fit Telegram's 4096-char message limit.

    Splits on paragraph boundaries (\n\n). If a single paragraph exceeds the
    limit, falls through to a hard char-cut on that paragraph.
    """
    if len(text.encode("utf-8")) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    current_size = 0
    for para in text.split("\n\n"):
        para_size = len((para + "\n\n").encode("utf-8"))
        if para_size > limit:
            # Flush whatever we have first
            if current:
                chunks.append("\n\n".join(current))
                current = []
                current_size = 0
            # Hard-cut the oversized paragraph
            data = para.encode("utf-8")
            for i in range(0, len(data), limit):
                chunks.append(data[i:i + limit].decode("utf-8", errors="ignore"))
            continue
        if current_size + para_size > limit and current:
            chunks.append("\n\n".join(current))
            current = [para]
            current_size = para_size
        else:
            current.append(para)
            current_size += para_size
    if current:
        chunks.append("\n\n".join(current))
    return chunks


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
