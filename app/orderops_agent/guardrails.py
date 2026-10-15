"""App-side input/output guardrails for the OrderOps copilot.

These run *in addition to* the Unity AI Gateway guardrails (jailbreak, PII, unsafe content),
which sit in front of the model. The app-side layer handles things the gateway can't know about:
  * the copilot is READ-ONLY - refuse write/mutation intents before spending tokens,
  * domain-specific PII (UPI VPAs, Aadhaar, PAN, Indian mobiles) in both directions,
  * groundedness - flag order IDs in the answer that never appeared in tool results.

Pure Python, no Databricks dependencies, so it is unit-tested in CI.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

MAX_INPUT_CHARS = 2000

ORDER_ID_RE = re.compile(r"\bORD-[0-9A-F]{8}\b", re.IGNORECASE)

INJECTION_PATTERNS = [
    r"ignore\s+(all\s+|any\s+|the\s+)?(previous|prior|above|earlier)\s+(instructions|rules|prompts?)",
    r"disregard\s+(your|the|all)\s+(instructions|rules|guidelines)",
    r"(reveal|show|print|repeat)\s+(me\s+)?(your|the)\s+(system\s+)?(prompt|instructions)",
    r"\byou\s+are\s+now\b",
    r"\b(developer|dan|jailbreak)\s+mode\b",
    r"pretend\s+(you\s+are|to\s+be)\s+(an?\s+)?(unrestricted|unfiltered)",
]

WRITE_INTENT_PATTERNS = [
    r"^\s*(please\s+|can\s+you\s+|could\s+you\s+)?(delete|drop|truncate|insert|alter|grant|revoke|cancel|refund|modify)\b",
    r"^\s*(please\s+|can\s+you\s+|could\s+you\s+)?update\s+(the\s+)?(order|status|table|record|row)",
    r"\b(drop|truncate|alter)\s+(table|schema|catalog|view)\b",
    r"\bdelete\s+from\b",
    r"\binsert\s+into\b",
    r"\bupdate\s+\w+\s+set\b",
]

# Order matters: emails before UPI VPAs (a VPA looks like an email without a TLD).
# Note "version 1234567890123" style business numbers survive because cards must pass Luhn.
PII_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("EMAIL", re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")),
    ("UPI_ID", re.compile(r"\b[\w.-]{2,}@[a-zA-Z]{2,}\b")),
    # Cards before Aadhaar: the first 12 digits of a 16-digit card would otherwise look like an Aadhaar
    ("CARD", re.compile(r"\b(?:\d[ -]?){13,19}\b")),
    ("AADHAAR", re.compile(r"\b[2-9]\d{3}[\s-]?\d{4}[\s-]?\d{4}\b")),
    ("PAN", re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")),
    ("PHONE", re.compile(r"(?<!\w)(?:\+?91[\s-]?)?[6-9]\d{9}\b")),
]


@dataclass
class GuardrailResult:
    allowed: bool
    text: str
    reasons: list[str] = field(default_factory=list)
    message: str | None = None  # user-facing explanation when blocked


def _luhn_ok(digits: str) -> bool:
    nums = [int(d) for d in digits][::-1]
    total = sum(n if i % 2 == 0 else (n * 2 - 9 if n * 2 > 9 else n * 2) for i, n in enumerate(nums))
    return total % 10 == 0


def redact_pii(text: str) -> tuple[str, list[str]]:
    """Mask PII. Card numbers are only masked if they pass a Luhn check (fewer false positives)."""
    found: list[str] = []

    def _sub(label: str, pattern: re.Pattern, s: str) -> str:
        def repl(m: re.Match) -> str:
            if label == "CARD":
                digits = re.sub(r"\D", "", m.group(0))
                if not (13 <= len(digits) <= 19 and _luhn_ok(digits)):
                    return m.group(0)
            found.append(label)
            return f"[{label}]"
        return pattern.sub(repl, s)

    for label, pattern in PII_PATTERNS:
        text = _sub(label, pattern, text)
    return text, sorted(set(found))


def check_input(question: str) -> GuardrailResult:
    q = (question or "").strip()
    if not q:
        return GuardrailResult(False, q, ["empty"], "Ask me something about orders - e.g. *Which West orders are breaching SLA?*")
    if len(q) > MAX_INPUT_CHARS:
        return GuardrailResult(False, q[:MAX_INPUT_CHARS], ["too_long"],
                               f"That's a bit long - please keep questions under {MAX_INPUT_CHARS} characters.")

    lowered = q.lower()
    if any(re.search(p, lowered) for p in INJECTION_PATTERNS):
        return GuardrailResult(False, q, ["prompt_injection"],
                               "I can't change my instructions. I can help you look up orders, SLAs and pipeline health.")
    if any(re.search(p, lowered) for p in WRITE_INTENT_PATTERNS):
        return GuardrailResult(False, q, ["write_intent"],
                               "I'm a read-only copilot - I can look up and explain orders, but changes "
                               "(cancel, refund, update, delete) must go through the order management system.")

    redacted, pii = redact_pii(q)
    return GuardrailResult(True, redacted, [f"pii_redacted:{p}" for p in pii])


def check_output(answer: str, evidence: list[str], system_prompt: str | None = None) -> GuardrailResult:
    """evidence = tool results + the user's (redacted) question; anything else is unverified."""
    text = answer or ""
    reasons: list[str] = []

    if system_prompt and len(system_prompt) > 80 and system_prompt[:80] in text:
        return GuardrailResult(False, "", ["prompt_leak"], "Sorry, I can't share that.")

    text, pii = redact_pii(text)
    reasons += [f"pii_redacted:{p}" for p in pii]

    seen = {m.upper() for e in evidence for m in ORDER_ID_RE.findall(e or "")}
    unverified = sorted({m.upper() for m in ORDER_ID_RE.findall(text)} - seen)
    if unverified:
        reasons.append("ungrounded_order_ids:" + ",".join(unverified))
        text += ("\n\n> ⚠️ I couldn't verify these order IDs against live data, treat them with caution: "
                 + ", ".join(unverified))

    return GuardrailResult(True, text, reasons)
