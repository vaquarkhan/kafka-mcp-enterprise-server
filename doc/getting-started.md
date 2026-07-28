# Getting started

## Requirements

- Python **3.8+**
- **No third-party packages** (stdlib only)

## Run the conformance suite

From the repository root:

```bash
python run_tests.py
```

Expect: `TOTAL: 85/85 passed, 0 failed`.

## Start over stdio (MCP host integration)

```bash
echo {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}} | python serve_stdio.py
```

Newline-delimited JSON-RPC on stdin; one JSON response per line on stdout.

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
