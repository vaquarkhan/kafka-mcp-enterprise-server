# Testing

## Conformance suite (85 checks)

```bash
python run_tests.py
```

| File | Category | Count |
|------|----------|-------|
| `tests/test_functional.py` | Functional / tools | 12 |
| `tests/test_security.py` | Security conformance | 19 |
| `tests/test_guardrails.py` | Data-protection | 14 |
| `tests/test_report_mechanisms.py` | KIP mechanisms | 13 |
| `tests/test_resources.py` | `kafka://` resources | 6 |
| `tests/test_integration_stdio.py` | stdio subprocess | 8 |
| `tests/test_audit_hardening.py` | Protocol + audit fixes (A1-B8/C) | 13 |

**Total = 85.** Expect: `TOTAL: 85/85 passed, 0 failed`.

## Quick smoke (separate)

```bash
python test_kafka_mcp.py
```

16 checks covering a subset of security controls (not part of the 85).

## Demo

```bash
python demo_end_to_end.py
```

22 numbered steps; expects security codes including `-32029` and `-32040`…`-32047`.

## Compile check

```bash
python -c "import py_compile,glob;[py_compile.compile(f,doraise=True) for f in glob.glob('kafka_mcp/*.py')+glob.glob('tests/*.py')+glob.glob('*.py')];print('compile OK')"
```

## Examples

```bash
python examples/01_sre_readonly_triage/run.py
python examples/02_multi_tenant_namespaces/run.py
python examples/03_change_window_approvals/run.py
python examples/04_pii_secret_egress_guard/run.py
python examples/05_blast_radius_resilience/run.py
python examples/06_identity_and_sensitive_topics/run.py
```
