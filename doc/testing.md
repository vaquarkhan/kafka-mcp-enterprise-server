# Testing

## Conformance suite (302 checks)

```bash
python run_tests.py
```

| File | Category | Count |
|------|----------|-------|
| `tests/test_functional.py` | Functional / tools | 12 |
| `tests/test_security.py` | Security conformance | 20 |
| `tests/test_guardrails.py` | Data-protection | 14 |
| `tests/test_report_mechanisms.py` | KIP mechanisms | 13 |
| `tests/test_resources.py` | `kafka://` resources | 6 |
| `tests/test_integration_stdio.py` | stdio subprocess | 8 |
| `tests/test_audit_hardening.py` | Protocol + audit fixes (A1-B8/C) | 13 |
| `tests/test_coverage_gaps.py` | Branch / module coverage gap-fill | 123 |
| `tests/test_kip_conformance.py` | KIP-1318 Test Plan + Phase-1 matrix | 93 |

**Total = 302.** Expect: `TOTAL: 302/302 passed, 0 failed`.

## Line coverage (optional)

Requires the `coverage` package (not a runtime dependency of `kafka_mcp/`):

```bash
pip install coverage
python run_coverage.py
```

Expect **100%** line coverage of `kafka_mcp/`. Config: [`.coveragerc`](../.coveragerc).

## KIP checklist

`tests/test_kip_conformance.py` maps each KIP Test Plan item and Phase-1 tool/resource to a check. Spec-only items (HTTP, Connect, EOS, live brokers) are asserted as **not registered**.

## Quick smoke (separate)

```bash
python test_kafka_mcp.py
```

16 checks covering a subset of security controls (not part of the 302).

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
