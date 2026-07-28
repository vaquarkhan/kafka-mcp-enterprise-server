# Observability & OpenTelemetry

## Do you need OpenTelemetry?

| Context | Recommendation |
|---------|----------------|
| **This Python reference** | **No** — not required. Use audit trail + `kafka://health` + structured error codes. |
| **Demos / CI / teaching** | Skip OTel; keep zero dependencies. |
| **Production Java (KIP-1318 / KAFKA-20436)** | **Yes, recommended** — metrics/traces for tool latency, denials, breaker state, identity. |
| **Python fork that fronts real Kafka** | Optional; add OTel at the host or wrap `handle()`. |

### Why not required here

1. Core package is **stdlib-only** (easy to audit and install).  
2. Reference already exposes **ALLOW/DENY audit** and **breaker health**.  
3. Official production path is **Java**; that is where first-class OTel belongs.

### Optional Python extra

```bash
pip install kafka-mcp-enterprise-kip1318[otel]
```

```python
from kafka_mcp.observability import get_tracer, otel_available

print(otel_available())  # True only if OTel packages are installed
tracer = get_tracer()
with tracer.start_as_current_span("tools_call"):
    ...
```

`get_tracer()` / `get_meter()` **no-op** when OpenTelemetry is not installed — safe defaults.
