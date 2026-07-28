# Publishing to PyPI

Package name: **`kafka-mcp-enterprise-kip1318`**  
Import name: **`kafka_mcp`**  
Console script: **`kafka-mcp-enterprise`**

This is a **community reference** for [KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka) / [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436). It is **not** the official Apache Kafka Java MCP module.

## Install (after publish)

```bash
pip install kafka-mcp-enterprise-kip1318
kafka-mcp-enterprise   # stdio MCP server
```

Optional OpenTelemetry APIs (not required):

```bash
pip install kafka-mcp-enterprise-kip1318[otel]
```

## Local build check

```bash
python -m pip install build twine
python -m build
python -m twine check dist/*
pip install dist/*.whl
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | kafka-mcp-enterprise
```

## Publish checklist

1. Bump `version` in `pyproject.toml` and `kafka_mcp/__init__.py`.  
2. `python run_tests.py` → **72/72**.  
3. `python -m build` and `twine check dist/*`.  
4. Create a PyPI account / API token.  
5. Test upload (optional): `twine upload --repository testpypi dist/*`  
6. Prod: `twine upload dist/*`  
7. Verify: `pip install kafka-mcp-enterprise-kip1318==<version>` on a clean venv.

## What is **not** in the wheel

- `tests/`, `examples/`, `doc/`, `internal/` — clone the repo for those.  
- Real Kafka brokers — in-memory backend only.
