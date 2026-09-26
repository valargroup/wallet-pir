#!/usr/bin/env python3
"""Execute a monitor with systemd's scoped Slack credential; never print it."""
import os
import sys
from pathlib import Path

credential = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "slack-webhook"
value = credential.read_text().strip()
if not value.startswith("https://hooks.slack.com/"):
    raise SystemExit("Invalid Slack credential")
os.environ["PIR_APM_SLACK_WEBHOOK_URL"] = value
os.execv(sys.argv[1], sys.argv[1:])
