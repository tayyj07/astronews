#!/usr/bin/env bash
# Install AstroNews systemd units. Run as root: `sudo bash deploy/install.sh`.
# Does NOT enable timers — do that manually after a test run, e.g.:
#     systemctl enable --now astronews-scrape.timer
#     systemctl enable --now astronews-notify.timer

set -euo pipefail

SRC=$(cd "$(dirname "$0")" && pwd)/systemd
DEST=/etc/systemd/system

if [[ $EUID -ne 0 ]]; then
  echo "must run as root" >&2
  exit 1
fi

install -m 644 "$SRC"/astronews-scrape.service        "$DEST/"
install -m 644 "$SRC"/astronews-scrape.timer          "$DEST/"
install -m 644 "$SRC"/astronews-notify.service        "$DEST/"
install -m 644 "$SRC"/astronews-notify.timer          "$DEST/"
install -m 644 "$SRC"/astronews-bot.service           "$DEST/"
install -m 644 "$SRC"/astronews-admin-report.service  "$DEST/"
install -m 644 "$SRC"/astronews-admin-report.timer    "$DEST/"

systemctl daemon-reload

echo "installed systemd units:"
ls -la "$DEST"/astronews-*

cat <<EOF

Next steps (after the credentials file is in place and a manual test passes):
  systemctl enable --now astronews-scrape.timer
  systemctl enable --now astronews-notify.timer
  systemctl enable --now astronews-bot.service
  systemctl enable --now astronews-admin-report.timer
  systemctl list-timers astronews-*
  systemctl status astronews-bot.service
EOF
