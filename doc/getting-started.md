# Getting started

## Requirements

- Python **3.8+**
- **No third-party packages** (stdlib only)

## Run the conformance suite

From the repository root:

```bash
python run_tests.py
```

Expect: `TOTAL: 302/302 passed, 0 failed`.

Optional line coverage (dev dependency):

```bash
pip install coverage
python run_coverage.py
```

Expect **100%** of `kafka_mcp/`.

Scope honesty (what is *not* in this reference): [kip-alignment.md](kip-alignment.md).

## Start over stdio (MCP host integration)

```bash
echo {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}} | python serve_stdio.py
```

Newline-delimited JSON-RPC on stdin; one JSON response per line on stdout.

Shipped default: **read/consume tools only**. To exercise mutate/destructive tools in a local demo:

```bash
set MCP_TOOLS_ALLOWED=*
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | python serve_stdio.py
```

List tools:

```bash
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | python serve_stdio.py
```

## First in-process tool call

```python
from kafka_mcp.config import Config
from kafka_mcp.server import KafkaMcpServer

server = KafkaMcpServer(Config(allowed_topic_prefixes=["agent."]))
session = {"identity": "demo"}

def call(tool, **arguments):
    return server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
        session,
    )

print(call("create_topic", name="agent.orders", partitions=3))
print(call("produce_message", topic="agent.orders", value="hello"))
print(call("consume_messages", topic="agent.orders", maxMessages=5))
```

## Next steps

- Walk security behavior: `python demo_end_to_end.py`
- Read [architecture](architecture.md) and [security controls](security-controls.md)
- Run a scenario from [examples](../examples/README.md)
- See what is **not** in this reference (and why): [kip-alignment.md](kip-alignment.md)

---

## Note: Python reference vs Java production code

This package is a **Python reference implementation** of the KIP-1318 security control model. It is **not** Java and is **not** the official Apache Kafka MCP server.

Missing KIP surface (HTTP transport, live brokers, EOS, Connect, distributed state, etc.) is intentional here. Those items are specified for the **actual Java production module** under [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436) and will be added in that code - not claimed as complete in this reference.
