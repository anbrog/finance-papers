#!/bin/bash
# Direct runner - no reinstall needed after code changes
cd "$(dirname "$0")"
python3 -m finance_papers.cli "$@"
