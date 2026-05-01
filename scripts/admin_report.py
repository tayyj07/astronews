#!/usr/bin/env python3
"""Daily admin report — fires at 18:00 SGT.

Sends a Telegram message to ADMIN_CHAT_IDS summarising the last 24h:
    - new atoms created (via Phase A classification of /add'd topics)
    - heuristic "possible duplicates" — atom pairs whose ids share a long
      common prefix or whose levenshtein distance is small (cheap stand-in
      for embedding similarity, given we have no embedding API)
    - topic events (adds/removes) per user

Reads `~/.config/astronews/credentials.env` for `TELEGRAM_BOT_TOKEN`.
"""

from __future__ import annotations

import datetime
import html
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as dbm  # noqa: E402

CREDENTIALS_FILE = Path(os.path.expanduser("~/.config/astronews/credentials.env"))
ADMIN_CHAT_IDS = [655916874]
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
LOOKBACK_HOURS = 24


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
    sys.exit("TELEGRAM_BOT_TOKEN missing")


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            cur.append(min(cur[j-1] + 1, prev[j] + 1, prev[j-1] + cost))
        prev = cur
    return prev[-1]


def find_possible_duplicates(atoms: list, threshold: int = 3) -> list[tuple]:
    """Return pairs of atom_ids whose levenshtein distance is small."""
    pairs = []
    ids = [a["atom_id"] for a in atoms]
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a, b = ids[i], ids[j]
            if abs(len(a) - len(b)) > threshold:
                continue
            d = levenshtein(a, b)
            # Heuristic: distance <= 3 AND distance < len(shorter)/2
            if d <= threshold and d < min(len(a), len(b)) / 2 + 1:
                pairs.append((a, b, d))
    return sorted(pairs, key=lambda p: p[2])


def send_message(token: str, chat_id: int, text: str) -> dict:
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def main() -> int:
    token = load_token()
    conn = dbm.connect()
    dbm.init_schema(conn)

    cutoff = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(hours=LOOKBACK_HOURS)
    ).isoformat(timespec="seconds")

    new_atoms = dbm.atoms_created_since(conn, cutoff)
    events = dbm.topic_events_since(conn, cutoff)
    all_atoms = dbm.all_atoms(conn)
    dup_pairs = find_possible_duplicates(all_atoms) if all_atoms else []

    # Build the message
    lines = [
        f"<b>AstroNews — admin report</b>",
        f"<i>Last {LOOKBACK_HOURS}h up to {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>",
        "",
        f"<b>Stats:</b> {len(all_atoms)} atoms total · {len(events)} topic events · {len(new_atoms)} new atoms",
    ]

    if new_atoms:
        lines.append("")
        lines.append("<b>New atoms (last 24h):</b>")
        for a in new_atoms:
            lines.append(f"• <code>{a['atom_id']}</code>")

    if dup_pairs:
        lines.append("")
        lines.append("<b>Possible duplicate atoms:</b>")
        for a, b, d in dup_pairs[:10]:
            lines.append(f"• <code>{a}</code> ↔ <code>{b}</code>  (edit distance {d})")
        lines.append("Use <code>/admin merge &lt;keep&gt; &lt;drop&gt;</code> to merge.")

    if not new_atoms and not dup_pairs:
        lines.append("")
        lines.append("<i>Nothing new since yesterday.</i>")

    text = "\n".join(lines)
    for admin_id in ADMIN_CHAT_IDS:
        try:
            resp = send_message(token, admin_id, text)
            if resp.get("ok"):
                print(f"sent to {admin_id}")
            else:
                print(f"telegram error to {admin_id}: {resp}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            print(f"send to {admin_id} failed: {type(e).__name__}: {e}", file=sys.stderr)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
