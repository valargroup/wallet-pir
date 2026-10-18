"""Canonical digests and crash-safe JSON files.

Both are byte-compatible with the Enhance expansion journal, whose persisted
digests and files predate this module: changing either would invalidate
recorded operations.
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile


def canonical(value):
    """Canonical JSON bytes: sorted keys, no whitespace, NaN and Infinity refused."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def atomic_json(path, value, prefix='.tmp-', mode=None):
    """Replace `path` with indented JSON, durable once this returns.

    The temporary file is created private (0600) in the same directory, so a
    reader sees either the old or the new file, never a partial one. `mode`
    widens it before the rename when a non-root reader needs it. The directory
    is fsynced so the rename itself survives a crash.
    """
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=prefix, dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as handle:
            json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write('\n')
            handle.flush()
            if mode is not None:
                os.fchmod(handle.fileno(), mode)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
