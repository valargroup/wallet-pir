#!/usr/bin/env python3
"""Inject only Wallet PIR runtime secrets from systemd's credential mount."""
import json
import os
from pathlib import Path
import sys

KEYS = frozenset({
    'DO_TOKEN_NEW_ORG', 'CF_API_TOKEN', 'WALLET_PIR_TF_STATE_ACCESS_KEY',
    'WALLET_PIR_TF_STATE_SECRET_KEY', 'WALLET_PIR_DEPLOY_SSH_KEY',
    'PIR_APM_SLACK_WEBHOOK_URL',
})


def credentials(directory):
    try:
        values = json.loads((Path(directory) / 'runtime').read_text())
        if not isinstance(values, dict) or set(values) != KEYS:
            raise ValueError()
        if any(not isinstance(value, str) or not value or '\0' in value
               for value in values.values()):
            raise ValueError()
        return values
    except (OSError, ValueError, TypeError):
        raise RuntimeError('Wallet PIR runtime credential is missing or invalid') from None


def main():
    if len(sys.argv) < 2 or not os.environ.get('CREDENTIALS_DIRECTORY'):
        raise RuntimeError('Run through the Wallet PIR systemd credential service')
    environment = dict(os.environ)
    environment.update(credentials(os.environ['CREDENTIALS_DIRECTORY']))
    os.execvpe(sys.argv[1], sys.argv[1:], environment)


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
