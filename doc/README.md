# Documentation

End-to-end guide for the Kafka MCP reference server.

1. [Getting started](getting-started.md) — run the server and call a tool  
2. [Architecture](architecture.md) — components, 3 Mermaid diagrams (client–host–server, topology, Direct Partition Assignment)  
3. [Security controls](security-controls.md) — fail-closed pipeline + 2 Mermaid diagrams (evaluation order, data-to-tool defense)  
4. [Configuration](configuration.md) — every config field  
5. [Tools & resources](tools-and-resources.md) — MCP surface  
6. [Error codes](error-codes.md) — operator / agent error map  
7. [Testing](testing.md) — conformance suite  
8. [KIP-1318 alignment](kip-alignment.md) — feature matrix + intentional gaps  
9. [Publishing to PyPI](publishing.md) — `kafka-mcp-enterprise`  
10. [Observability / OpenTelemetry](observability.md) — optional, not required  
11. [Agents & skills](agents-and-skills.md) — Cursor, Kiro, ChatGPT, Gemini, Copilot  

Hands-on scripts: [../examples/](../examples/README.md)

## Product images

![Product image](assets/kafka-mcp-banner.png)

![What’s included](assets/kafka-mcp-features.png)

These are documentation packaging images only — this package has no web UI. Sources: [`kafka-mcp-banner.svg`](assets/kafka-mcp-banner.svg), [`kafka-mcp-features.svg`](assets/kafka-mcp-features.svg). Architecture Mermaid diagrams: [architecture.md](architecture.md) · [security-controls.md](security-controls.md).
