import re


_HEADER_SECRET_RE = re.compile(
    r"(?im)(\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:\s*)[^\r\n]*"
)
_FIELD_SECRET_RE = re.compile(
    r"""(?i)(["']?(?:x?-?csrf[-_]?token|api[_-]?key|password|passwd|secret|access[_-]?token|refresh[_-]?token|username|user_name|cookie)["']?\s*[:=]\s*["']?(?:Bearer\s+)?)([^"'\s,;<>]+)"""
)


def redact_sensitive_text(value: object, *, limit: int = 2000) -> str:
    """Redact common credential and session-token forms before exposing diagnostics."""
    text = str(value or "")
    text = _HEADER_SECRET_RE.sub(r"\1[REDACTED]", text)
    text = _FIELD_SECRET_RE.sub(r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*",
        "Bearer [REDACTED]",
        text,
    )
    return text[:limit]
