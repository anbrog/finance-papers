#!/usr/bin/env bash
# Unload and remove all finance-papers LaunchAgents (articles + working-papers).
# Idempotent.
set -euo pipefail

DOMAIN="gui/$(id -u)"
LABELS=(
    "com.andreasbrogger.finance-papers"
    "com.andreasbrogger.finance-papers-w"
)

removed=0
for LABEL in "${LABELS[@]}"; do
    PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
    if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
        launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
        echo "Unloaded: $LABEL"
        removed=1
    fi
    if [[ -f "$PLIST" ]]; then
        rm -f "$PLIST"
        echo "Deleted:  $PLIST"
        removed=1
    fi
done

if [[ $removed -eq 0 ]]; then
    echo "No finance-papers LaunchAgents found."
fi
