"""Shared helpers for production-grade example scenarios."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kafka_mcp.config import Config  # noqa: E402
from kafka_mcp.server import KafkaMcpServer  # noqa: E402


def banner(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def step(n: int, msg: str) -> None:
    print(f"\n[{n}] {msg}")


def call(
    server: KafkaMcpServer,
    tool: str,
    arguments: Optional[Dict[str, Any]] = None,
    session: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments or {}},
        },
        session if session is not None else {},
    )


def rpc(
    server: KafkaMcpServer,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    session: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
        session if session is not None else {},
    )


def expect_ok(resp: Dict[str, Any], label: str) -> bool:
    ok = "result" in resp and "error" not in resp
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        print(f"         {resp}")
    return ok


def expect_code(resp: Dict[str, Any], code: int, label: str) -> bool:
    got = (resp.get("error") or {}).get("code")
    ok = got == code
    print(f"  {'PASS' if ok else 'FAIL'}  {label} (got {got})")
    if not ok:
        print(f"         {resp}")
    return ok


def finish(ok: bool) -> None:
    print()
    if ok:
        print("Example completed successfully.")
        sys.exit(0)
    print("Example failed — see FAIL lines above.")
    sys.exit(1)


def data_dir(example_file: str) -> Path:
    """Return the `data/` directory next to an example's run.py."""
    return Path(example_file).resolve().parent / "data"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        rows.append(json.loads(line))
    return rows


def seed_topic_from_jsonl(
    server: KafkaMcpServer,
    topic: str,
    path: Path,
    *,
    partitions: int = 1,
    value_field: Optional[str] = None,
) -> int:
    """Create topic (if needed) and produce each JSONL row as a record value."""
    if topic not in server.backend.topics:
        server.backend.create_topic(topic, partitions=partitions)
    rows = load_jsonl(path)
    for row in rows:
        if value_field:
            value = row.get(value_field, row)
        else:
            value = row
        payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
        key = row.get("_key") if isinstance(row, dict) else None
        server.backend.produce(topic, value=payload, key=str(key) if key is not None else None)
    return len(rows)
