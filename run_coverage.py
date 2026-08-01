#!/usr/bin/env python3
"""Run conformance suite under coverage and print a report. Dev aid (optional)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    try:
        import coverage
    except ImportError:
        print("Install coverage: pip install coverage", file=sys.stderr)
        return 2

    cov = coverage.Coverage(source=["kafka_mcp"], config_file=str(ROOT / ".coveragerc"))
    cov.start()
    try:
        from run_tests import main as run_main

        rc = run_main()
    finally:
        cov.stop()
        cov.save()

    print()
    total = cov.report(show_missing=True)
    if total < 100.0:
        print(f"\nCoverage {total:.1f}% < 100%.", file=sys.stderr)
        return 1 if rc == 0 else rc
    print(f"\nCoverage {total:.1f}%")
    return rc


if __name__ == "__main__":
    sys.exit(main())
