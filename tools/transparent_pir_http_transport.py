"""HTTP transport for the transparent-history retrieval service.

Implements the same `fetch(folder, manifest, table, rows)` contract as
`FileTransport` and `PirTransport` in `transparent_pir_incremental.py`, so
`sync` runs unchanged against a deployed service. The difference is what the
byte counts mean: `PirTransport` shells out to an in-process benchmark and
charges the application payload it reports, while this charges what actually
crossed a socket, including request and response framing.

Requires a running `transparent-history-server` serving the same generation as
`folder`. The generation identity is checked rather than assumed: a service
serving a different generation would return rows that decode to the wrong
history, and silently comparing those bytes would produce a measurement of
nothing.
"""

import base64
import json
import subprocess
import time
import urllib.request
from pathlib import Path


class HttpTransportError(RuntimeError):
    pass


class HttpPirTransport:
    """Fetch rows from a deployed transparent-history service.

    `pad_to` fixes the number of queries issued per call. Real selections come
    first and padding queries follow, and every one of them is executed and
    decoded, so the service and the network see a constant number of
    indistinguishable requests. A caller that lets the count track the number of
    matches publishes how many blocks matched, which is most of what the local
    filter match was for.
    """

    def __init__(self, base_url, client_binary, pad_to=None, timeout=600):
        self.base_url = base_url.rstrip("/")
        self.client_binary = Path(client_binary).resolve()
        self.pad_to = pad_to
        self.timeout = timeout
        self.calls = []
        self.session = None
        # Batches and key uploads separate the reuse saving from the totals:
        # packing keys travel once per batch, so queries-per-batch is what the
        # measurement is actually varying.
        self.batches = 0
        self.key_upload_bytes = 0
        self.setup_charged = False

    def _init(self):
        if self.session is None:
            request = urllib.request.Request(f"{self.base_url}/v1/transparent-history/init")
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                self.session = json.loads(response.read())
        return self.session

    def fetch(self, folder, manifest, table, rows):
        if table not in ("directory", "pages"):
            raise HttpTransportError(f"unknown table {table!r}")
        session = self._init()

        # The service must be serving this generation. Comparing bytes retrieved
        # from a different generation against this one's expectations would
        # compare unrelated data.
        geometry = manifest[table]
        served = session[table]["generation"]
        if int(served["rows"]) != geometry["rows"] or int(served["row_bytes"]) != geometry["row_bytes"]:
            raise HttpTransportError(
                f"service serves {table} as {served['rows']}x{served['row_bytes']}, "
                f"generation says {geometry['rows']}x{geometry['row_bytes']}"
            )

        pad_to = len(rows) if self.pad_to is None else self.pad_to
        if len(rows) > pad_to:
            raise HttpTransportError(
                f"{len(rows)} selections exceed the fixed query budget of {pad_to}"
            )

        started = time.monotonic()
        request = {
            "base_url": self.base_url,
            "table": table,
            "rows": list(rows),
            "pad_to": pad_to,
        }
        completed = subprocess.run(
            [str(self.client_binary), "fetch"],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            check=False,
            timeout=self.timeout,
        )
        if completed.returncode != 0:
            raise HttpTransportError(f"fetch failed: {completed.stderr.strip()}")
        result = json.loads(completed.stdout)
        wall_ms = (time.monotonic() - started) * 1000.0

        # Cross-check the setup charge against the session this transport
        # fetched itself. The client reports what it downloaded; recomputing it
        # from an independent copy catches a units mistake, which is otherwise
        # invisible because a wrong-but-small number still looks like a number.
        expected_setup = sum(
            len(part)
            for table_session in (session["directory"], session["pages"])
            for part in table_session["public_params"]
        )
        if result["setup_download_bytes"] != expected_setup:
            raise HttpTransportError(
                f"setup charge {result['setup_download_bytes']} does not match the "
                f"{expected_setup} bytes of published parameters in the session"
            )

        decoded = {
            int(row): base64.b64decode(value)
            for row, value in result["rows"].items()
        }
        if set(decoded) != set(rows):
            raise HttpTransportError("response coverage mismatch")
        for row, value in decoded.items():
            if len(value) != geometry["row_bytes"]:
                raise HttpTransportError(f"row {row} is {len(value)} bytes")

        self.calls.append((table, list(rows), pad_to))
        self.batches += result.get("batches", 0)
        self.key_upload_bytes += result.get("key_upload_bytes", 0)
        # Charge the published parameters once per sync, not once per call.
        # Each call runs a fresh CLI process that reconnects, but a wallet holds
        # one session and caches public parameters across it; billing every
        # batch for them would invent setup traffic a real client never sends.
        if self.setup_charged:
            setup_bytes = 0
        else:
            setup_bytes = result["setup_download_bytes"]
            self.setup_charged = True

        cost = {
            "queries": result["queries"],
            # The published parameters are fetched once per session and are
            # public, so they are charged to the session rather than to a call.
            # With key reuse the server publishes one set per batch slot, so
            # this is where reuse costs what it saves on uploads.
            "setup_download_bytes": setup_bytes,
            "upload_bytes": result["upload_bytes"],
            "response_bytes": result["download_bytes"],
            "core_ms": wall_ms,
        }
        # Deliberately not a cost key: sync accumulates every key it is handed
        # into the wallet's running cost, and an unknown one aborts the sync.
        # Padding is recorded on the transport instead, in self.calls.
        return decoded, cost
