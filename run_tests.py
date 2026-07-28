#!/usr/bin/env python3
"""Master test runner — expects TOTAL: 72/72 passed, 0 failed."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests import (  # noqa: E402
    test_functional,
    test_guardrails,
    test_integration_stdio,
    test_report_mechanisms,
    test_resources,
    test_security,
)


def main() -> int:
    suites = [
        test_functional,
        test_security,
        test_guardrails,
        test_report_mechanisms,
        test_resources,
        test_integration_stdio,
    ]
    total_pass = 0
    total_fail = 0
    for mod in suites:
        print(f"\n=== {mod.__name__} ===")
        checker = mod.run()
        p, f = checker.summary()
        total_pass += p
        total_fail += f
        print(f"  subtotal: {p} passed, {f} failed")

    print(f"\nTOTAL: {total_pass}/{total_pass + total_fail} passed, {total_fail} failed")
    return 0 if total_fail == 0 and total_pass == 72 else 1


if __name__ == "__main__":
    sys.exit(main())
