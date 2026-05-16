#!/usr/bin/env bash
# Install a LaunchAgent that runs `finance-papers -Nqp[w]` at the top of every
# hour and appends output to ~/logs/finance-papers.log. Idempotent.
#
# Usage:
#   notify_on.sh                  # articles  -> finance-papers -Nqp
#   notify_on.sh articles         # same as above
#   notify_on.sh working-papers   # working papers -> finance-papers -Nqpw
set -euo pipefail

MODE="${1:-articles}"
case "$MODE" in
    articles)
        SUFFIX=""
        FLAGS="-Nqp"
        NTFY_TOPIC_VAL="finance-papers"
        LOG_NAME="finance-papers.log"
        ;;
    working-papers|wp|w)
        SUFFIX="-w"
        FLAGS="-Nqpw"
        NTFY_TOPIC_VAL="finance-papers-w"
        LOG_NAME="finance-papers-w.log"
        ;;
    *)
        echo "Error: unknown mode '$MODE' (expected 'articles' or 'working-papers')." >&2
        exit 2
        ;;
esac

LABEL="com.andreasbrogger.finance-papers${SUFFIX}"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/logs"
LOG_FILE="$LOG_DIR/$LOG_NAME"

BIN="$(command -v finance-papers || true)"
if [[ -z "$BIN" ]]; then
    echo "Error: 'finance-papers' not found on PATH." >&2
    echo "Install with: pip install -e ." >&2
    exit 1
fi

mkdir -p "$LOG_DIR" "$(dirname "$PLIST")"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>            <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BIN</string>
    <string>$FLAGS</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Minute</key>         <integer>0</integer>
  </dict>
  <key>RunAtLoad</key>        <false/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key><string>1</string>
  </dict>
  <key>StandardOutPath</key>  <string>$LOG_FILE</string>
  <key>StandardErrorPath</key><string>$LOG_FILE</string>
</dict>
</plist>
EOF

DOMAIN="gui/$(id -u)"

if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
fi
launchctl bootstrap "$DOMAIN" "$PLIST"

echo "Installed LaunchAgent: $LABEL"
echo "  Plist:      $PLIST"
echo "  Runs:       every hour on the minute (Minute=0)"
echo "  Command:    $BIN $FLAGS"
echo "  ntfy topic: $NTFY_TOPIC_VAL"
echo "  Log file:   $LOG_FILE"
echo "Remove with: finance-papers notify off"
