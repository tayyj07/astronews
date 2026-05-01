#!/usr/bin/env python3
"""AstroNews atom classifier — VPS-local, calls Anthropic API directly.

Reads `state/astronews.db` (VPS-local; not in git) for any user_topics
where `classified_at IS NULL`. For each, asks Claude Haiku 4.5 to decompose
into atomic facets (1-3 include + 0-2 exclude) and returns structured JSON.
Persists atoms + topic_facets + sets classified_at.

Designed to be called from a systemd timer every 15 minutes. Idempotent —
exits cleanly if no unclassified topics. Holds the shared flock so it
serializes with bot/notifier writes.

Requires `~/.config/astronews/credentials.env` with `ANTHROPIC_API_KEY=...`.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db as dbm  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_FILE = Path(os.path.expanduser("~/.astronews.lock"))
CREDENTIALS_FILE = Path(os.path.expanduser("~/.config/astronews/credentials.env"))

MODEL = "claude-haiku-4-5"
MAX_TOPICS_PER_RUN = 25  # safety cap; classify in batches if more arrive

SYSTEM_PROMPT = """You are an atom classifier for AstroNews, a multi-user news \
digest bot. Your job is to decompose a user-submitted topic string into atomic \
facets (tags) that can later be combined to drive news searches.

Atom kinds:
- subject: the news IS about this thing (bitcoin, ethereum, stablecoins, defi, ai-agents)
- context: where/who (tradfi, banking, regulation, government, retail, institutional)
- angle: how/what aspect (trends, security, hack, launch, partnership, regulation-update, fundraising, ipo)
- project: specific protocol/product names (megaeth, hyperliquid, kelpdao)
- other: escape hatch when nothing else fits

Rules:
- Decompose into 1 to 3 INCLUDE atoms and 0 to 2 EXCLUDE atoms.
- Prefer existing atoms. Create a new atom ONLY when no existing atom captures \
a unique aspect of the topic.
- Be conservative on exclusions — add an exclude atom only if user wording \
explicitly narrows scope (e.g. "stablecoins in tradfi" implies excluding defi).
- Atom IDs are kebab-case (lowercase, hyphens). e.g. `stablecoins`, `tradfi`, \
`project-launch`, `megaeth`.
- Keep atom descriptions short (under 80 chars).
- Include a one-sentence rationale (the user can /retag if it looks wrong).

Output ONLY a JSON object — no surrounding prose, no markdown code fences:
{
  "include_atoms": ["atom-id-1", "atom-id-2"],
  "exclude_atoms": [],
  "new_atoms": [{"atom_id": "...", "kind": "subject|context|angle|project|other", "description": "..."}],
  "rationale": "one-sentence explanation"
}
"""


def load_api_key() -> str:
    if not CREDENTIALS_FILE.exists():
        sys.exit(f"missing credentials at {CREDENTIALS_FILE}")
    for raw in CREDENTIALS_FILE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == "ANTHROPIC_API_KEY":
            return v.strip().strip('"').strip("'")
    sys.exit(f"ANTHROPIC_API_KEY missing from {CREDENTIALS_FILE}")


def format_atoms_for_prompt(atoms: list) -> str:
    if not atoms:
        return "(none yet — propose new atoms freely)"
    lines = []
    for a in atoms:
        lines.append(f"  - {a['atom_id']} [{a['kind']}]: {a['description']}")
    return "\n".join(lines)


@contextlib.contextmanager
def repo_lock():
    LOCK_FILE.touch(exist_ok=True)
    f = open(LOCK_FILE, "r+")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


def parse_response_json(text: str) -> dict:
    """Extract JSON from Claude's response. Handles plain JSON or fenced code."""
    text = text.strip()
    # Strip ``` fences if present
    fence = re.match(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    return json.loads(text)


def classify_topic(client, topic: str, atoms: list) -> dict:
    user_msg = (
        f'Topic to classify: "{topic}"\n\n'
        f"Existing atoms (use these when possible):\n"
        f"{format_atoms_for_prompt(atoms)}\n\n"
        f"Return only JSON."
    )
    resp = client.messages.create(
        model=MODEL,
        max_tokens=600,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = resp.content[0].text
    return parse_response_json(text)


def get_client():
    """Return an anthropic.Anthropic client using the credentials file.
    Raises if the SDK or key is missing."""
    api_key = load_api_key()
    os.environ["ANTHROPIC_API_KEY"] = api_key
    import anthropic  # noqa: PLC0415
    return anthropic.Anthropic()


def classify_and_persist(conn, client, chat_id: int, topic: str) -> dict:
    """Classify ONE topic and write the results to the DB. Returns a dict:
        {"include": [...], "exclude": [...], "new_atoms": [atom_id, ...],
         "rationale": "..."}
    The caller is expected to be holding the flock and to have already
    inserted the user_topics row."""
    atoms = [dict(r) for r in dbm.all_atoms(conn)]
    result = classify_topic(client, topic, atoms)

    include = result.get("include_atoms", []) or []
    exclude = result.get("exclude_atoms", []) or []
    rationale = result.get("rationale", "") or ""
    new_atom_ids: list[str] = []
    for new_atom in result.get("new_atoms", []) or []:
        aid = new_atom.get("atom_id")
        kind = new_atom.get("kind", "other")
        desc = new_atom.get("description", "")
        if not aid:
            continue
        if dbm.upsert_atom(conn, aid, kind, desc, created_by=chat_id):
            new_atom_ids.append(aid)

    dbm.set_facets(conn, chat_id, topic, include, exclude, rationale)
    return {
        "include": include,
        "exclude": exclude,
        "new_atoms": new_atom_ids,
        "rationale": rationale,
    }


def main() -> int:
    client = get_client()
    with repo_lock():
        conn = dbm.connect()
        dbm.init_schema(conn)

        unclassified = dbm.unclassified_topics(conn)
        if not unclassified:
            print("No unclassified topics; exiting.")
            conn.close()
            return 0

        unclassified = unclassified[:MAX_TOPICS_PER_RUN]
        print(f"Classifying {len(unclassified)} topic(s)")

        new_atom_ids: set[str] = set()
        classified_count = 0
        errors: list[str] = []

        for row in unclassified:
            chat_id = row["chat_id"]
            topic = row["topic"]
            print(f"  → {chat_id} / {topic!r}")
            try:
                info = classify_and_persist(conn, client, chat_id, topic)
            except Exception as e:  # noqa: BLE001
                msg = f"{type(e).__name__}: {e}"
                print(f"    classify failed: {msg}")
                errors.append(f"{topic}: {msg}")
                continue
            new_atom_ids.update(info["new_atoms"])
            classified_count += 1
            print(f"    include={info['include']} exclude={info['exclude']}  ({info['rationale'][:80]})")

        conn.close()

    summary_bits = [f"{classified_count} topic(s)"]
    if new_atom_ids:
        summary_bits.append(f"{len(new_atom_ids)} new atom(s) ({', '.join(sorted(new_atom_ids))})")
    if errors:
        summary_bits.append(f"{len(errors)} error(s)")
    print(f"\nOK: classified {'; '.join(summary_bits)}.")
    if errors:
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
    return 1 if errors and classified_count == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
