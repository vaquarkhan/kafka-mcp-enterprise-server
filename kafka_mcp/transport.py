"""stdio transport + HTTP notes."""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional, TextIO


def serve_stdio(
    server: Any,
    session: Optional[Dict[str, Any]] = None,
    stdin: Optional[TextIO] = None,
    stdout: Optional[TextIO] = None,
) -> None:
    """Read newline-delimited JSON-RPC from stdin; write JSON responses.

    HTTP / Streamable HTTP transport (not implemented in this reference):
    - Same JSON-RPC methods over POST (stateless preferred)
    - Bearer validation via auth.validate_bearer when audience/issuer configured
    - Approval tokens are HMAC self-contained; rate/breaker/quarantine remain
      per-process unless a shared store is added (see resilience.py)
    """
    session = session if session is not None else {}
    # Authoritative identity must come from the transport/session — never tool args.
    session.setdefault("identity", "stdio-user")
    inp = stdin or sys.stdin
    out = stdout or sys.stdout
    for line in inp:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            resp = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": "Parse error"},
            }
            out.write(json.dumps(resp) + "\n")
            out.flush()
            continue
        resp = server.handle(req, session)
        if resp is not None:
            out.write(json.dumps(resp, default=str) + "\n")
            out.flush()
