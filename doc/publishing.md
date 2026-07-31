# Publishing to PyPI (Trusted Publishing)

Package name: **`kafka-mcp-enterprise`**  
Import name: **`kafka_mcp`**  
Console script: **`kafka-mcp-enterprise`**  
Workflow: [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) (same pattern as [mcp-test-harness](https://github.com/vaquarkhan/mcp-test-harness/blob/main/.github/workflows/publish.yml))

This is a **community reference** for [KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka) / [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436). It is **not** the official Apache Kafka Java MCP module.

## Install (after publish)

```bash
pip install kafka-mcp-enterprise
kafka-mcp-enterprise   # stdio MCP server
```

Optional OpenTelemetry APIs (not required):

```bash
pip install kafka-mcp-enterprise[otel]
```

---

## Do you need Trusted Publishing?

**Yes - use Trusted Publishing (OIDC).** Do **not** put a long-lived `PYPI_API_TOKEN` in repo secrets unless you have a special reason.

Trusted Publishing is what `mcp-test-harness` uses: GitHub Actions gets a short-lived token from PyPI via OIDC (`permissions: id-token: write` + environment `pypi`).

### One-time setup (you must click these - automation cannot)

#### 1) GitHub Environment

1. Open https://github.com/vaquarkhan/kafka-mcp-enterprise-server/settings/environments  
2. **New environment** → name it exactly: **`pypi`**  
3. Optional: add required reviewers / wait timer for safer releases.

#### 2) PyPI Trusted Publisher

1. Log in at https://pypi.org (create account if needed).  
2. If the project does **not** exist yet: **Account settings → Publishing → Add a new pending publisher**.  
   If it already exists: open the project → **Publishing** → add publisher.  
3. Fill in **exactly**:

| Field | Value |
|-------|--------|
| PyPI project name | `kafka-mcp-enterprise` |
| Owner | `vaquarkhan` |
| Repository name | `kafka-mcp-enterprise-server` |
| Workflow name | `publish.yml` |
| Environment name | blank / **(any)** - or `pypi` if you pin it to the GitHub `pypi` environment |

4. Save. Docs: https://docs.pypi.org/trusted-publishers/

#### 3) Optional TestPyPI

Repeat on https://test.pypi.org with environment `testpypi` only if you add a TestPyPI job later. Not required for the current workflow.

### What you do **not** need

- ❌ `PYPI_API_TOKEN` / `TWINE_PASSWORD` repo secrets (when Trusted Publishing is set up)  
- ❌ Manual `twine upload` from a laptop for normal releases  

---

## Each release

1. Bump `version` in `pyproject.toml` **and** `kafka_mcp/__init__.py` (keep them equal).  
2. Commit and push to `main`.  
3. Confirm **CI** is green (`.github/workflows/ci.yml`).  
4. Tag and push:

```bash
git tag v0.1.0
git push origin v0.1.0
```

5. Watch **Actions → publish** - runs 85 tests, builds wheel/sdist, optional SBOM, then publishes via OIDC.  
6. Verify: https://pypi.org/project/kafka-mcp-enterprise/

Tag must match a version you intend to publish; PyPI rejects re-uploading the same version.

Or republish the current `main` tip without a new tag:

```bash
gh workflow run publish.yml -f confirm=publish
```

### Troubleshooting: `400 Non-user identities cannot create new projects`

Trusted Publishing worked, but the **pending publisher project name** did not match `pyproject.toml` `[project].name`, or the **GitHub repository** field did not match the renamed repo.

Live Trusted Publisher must be:

| Field | Value |
|-------|--------|
| PyPI project name | `kafka-mcp-enterprise` |
| Repository | `vaquarkhan/kafka-mcp-enterprise-server` |
| Workflow | `publish.yml` |

1. PyPI → project (or Account → **Publishing**) → edit/recreate the publisher.  
2. Re-run: Actions → **publish** → **Re-run failed jobs**, or `gh workflow run publish.yml -f confirm=publish`.

---

## Local build check (optional)

```bash
python -m pip install build twine
python -m build
python -m twine check dist/*
```

## What is **not** in the wheel

- `tests/`, `examples/`, `doc/`, `internal/` - clone the repo for those.  
- Real Kafka brokers - in-memory backend only.
