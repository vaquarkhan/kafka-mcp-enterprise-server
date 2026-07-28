"""DLP engine: 10 detectors, Luhn for cards, redact/block modes."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple


def _luhn_ok(digits: str) -> bool:
    nums = [int(c) for c in digits if c.isdigit()]
    if len(nums) < 13 or len(nums) > 19:
        return False
    checksum = 0
    parity = len(nums) % 2
    for i, n in enumerate(nums):
        if i % 2 == parity:
            n *= 2
            if n > 9:
                n -= 9
        checksum += n
    return checksum % 10 == 0


# Detector category -> (regex, mask token)
DETECTORS: Dict[str, Tuple[re.Pattern, str]] = {
    "email": (
        re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
        "[REDACTED_EMAIL]",
    ),
    "ssn": (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[REDACTED_SSN]"),
    "credit_card": (
        re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
        "[REDACTED_CARD]",
    ),
    "phone": (
        re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
        "[REDACTED_PHONE]",
    ),
    "ipv4": (
        re.compile(
            r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}"
            r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
        ),
        "[REDACTED_IP]",
    ),
    "aws_access_key": (
        re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        "[REDACTED_AWS_KEY]",
    ),
    "private_key": (
        re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
        "[REDACTED_PRIVATE_KEY]",
    ),
    "jwt": (
        re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
        "[REDACTED_JWT]",
    ),
    "iban": (
        re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b"),
        "[REDACTED_IBAN]",
    ),
    "secret_assignment": (
        re.compile(
            r"(?i)\b(?:password|passwd|secret|token|api[_-]?key|credential)\s*[=:]\s*\S+"
        ),
        "[REDACTED_SECRET]",
    ),
}


def scan(text: str) -> Set[str]:
    """Return set of detector categories that match text."""
    if not text:
        return set()
    hits: Set[str] = set()
    for cat, (pat, _) in DETECTORS.items():
        if cat == "credit_card":
            for m in pat.finditer(text):
                digits = re.sub(r"\D", "", m.group(0))
                if _luhn_ok(digits):
                    hits.add(cat)
                    break
        else:
            if pat.search(text):
                hits.add(cat)
    return hits


def redact(text: str) -> str:
    """Replace all detected sensitive spans with mask tokens."""
    if not text:
        return text
    out = text
    # Apply longer / more specific detectors first to avoid partial clobber
    order = [
        "private_key",
        "jwt",
        "aws_access_key",
        "secret_assignment",
        "iban",
        "email",
        "ssn",
        "credit_card",
        "phone",
        "ipv4",
    ]
    for cat in order:
        pat, mask = DETECTORS[cat]
        if cat == "credit_card":

            def _sub_card(m: re.Match) -> str:
                digits = re.sub(r"\D", "", m.group(0))
                return mask if _luhn_ok(digits) else m.group(0)

            out = pat.sub(_sub_card, out)
        else:
            out = pat.sub(mask, out)
    return out


class Dlp:
    """DLP processor with mode and block categories."""

    def __init__(
        self,
        mode: str = "redact",
        block_categories: Optional[List[str]] = None,
        *,
        redact_ipv4: bool = False,
    ) -> None:
        self.mode = mode or "redact"
        self.block_categories = set(block_categories or [])
        self.redact_ipv4 = bool(redact_ipv4)

    def _active_categories(self) -> List[str]:
        cats = [
            "private_key",
            "jwt",
            "aws_access_key",
            "secret_assignment",
            "iban",
            "email",
            "ssn",
            "credit_card",
            "phone",
        ]
        if self.redact_ipv4:
            cats.append("ipv4")
        return cats

    def process(self, text: str) -> Tuple[str, Set[str], bool]:
        """Return (redacted_text, hits, blocked).

        blocked = hits intersect block_categories OR (mode=="block" and hits).
        """
        if self.mode == "off" or text is None:
            return text if text is not None else "", set(), False
        active = set(self._active_categories())
        hits = scan(text) & active
        blocked = bool(hits & self.block_categories) or (
            self.mode == "block" and bool(hits)
        )
        if self.mode in ("redact", "block"):
            redacted = self._redact_active(text, active)
        else:
            redacted = text
        return redacted, hits, blocked

    def _redact_active(self, text: str, active: Set[str]) -> str:
        out = text
        order = [
            "private_key",
            "jwt",
            "aws_access_key",
            "secret_assignment",
            "iban",
            "email",
            "ssn",
            "credit_card",
            "phone",
            "ipv4",
        ]
        for cat in order:
            if cat not in active:
                continue
            pat, mask = DETECTORS[cat]
            if cat == "credit_card":

                def _sub_card(m: re.Match) -> str:
                    digits = re.sub(r"\D", "", m.group(0))
                    return mask if _luhn_ok(digits) else m.group(0)

                out = pat.sub(_sub_card, out)
            else:
                out = pat.sub(mask, out)
        return out
