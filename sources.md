# Watched sources — agent fetches each URL on every run and surfaces NEW posts.
# Format:  <url>  [→ <topic>]
# - With `→ <topic>` (matching a line in topics.md), new posts go under that topic's H2 in the digest.
# - Without it, new posts go under a final `## Watched sources` section.
# - Lines starting with `#` and blank lines are ignored.
# - First time a URL appears here, the agent SEEDS (records current posts as seen, surfaces nothing).
#   Subsequent runs surface only newly-published posts.

https://a16zcrypto.com/posts/tags/stablecoins/  → stablecoins development in the tradfi space
