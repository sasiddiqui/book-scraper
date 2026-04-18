"""
Title-key normalization for the author-enrichment cache.

The goal is a stable, low-cardinality key so equivalent titles across stores collapse
to the same cache row. Kept deliberately aggressive because false collisions across
unrelated titles are still fine here - the worst case is one extra LLM call.

Shared separately from `enrich_authors.py` so the same key can be reused by a
future cover-image enrichment pass (key by `titleKey + coverHash`).
"""

from __future__ import annotations

import re
import unicodedata


_ARABIC_HAMZA_MAP = {
    "أ": "ا",
    "إ": "ا",
    "آ": "ا",
    "ٱ": "ا",
    "ؤ": "و",
    "ئ": "ي",
}

_HARAKAT_RE = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]")
_LEADING_AL_RE = re.compile(r"\bال")

_PUNCT_RE = re.compile(r"[^\w\s\u0600-\u06FF]+", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")

_LEADING_ARTICLE_RE = re.compile(r"^(?:the|a|an|al[-\s]?)\s+", flags=re.IGNORECASE)

# trailing noise: edition / volume / part markers and bracketed parentheticals
_EDITION_RE = re.compile(
    r"\b("
    r"\d+(?:st|nd|rd|th)?\s*(?:ed(?:ition)?|edn|print(?:ing)?)|"
    r"(?:vol(?:ume)?|v|part|pt|no|number|book)\s*\.?\s*\d+|"
    r"hardcover|paperback|hardback|softcover|hc|pb|pbk|hbk|"
    r"revised|updated|new|full\s*color|pocket\s*size|deluxe"
    r")\b",
    flags=re.IGNORECASE,
)
_PARENS_RE = re.compile(r"\([^)]*\)|\[[^\]]*\]|\{[^}]*\}")


def _strip_arabic_noise(text: str) -> str:
    for src, dst in _ARABIC_HAMZA_MAP.items():
        text = text.replace(src, dst)
    text = _HARAKAT_RE.sub("", text)
    text = _LEADING_AL_RE.sub("", text)
    return text


def title_key(title: str | None) -> str | None:
    """
    Produce a stable, comparable key for a book title.

    Returns None for empty input so callers can short-circuit. The key is not meant
    to be human-readable - it is only a cache/index key.
    """
    if not title:
        return None

    normalized = unicodedata.normalize("NFKC", title).strip()
    if not normalized:
        return None

    normalized = _PARENS_RE.sub(" ", normalized)
    normalized = _strip_arabic_noise(normalized)
    normalized = normalized.lower()

    normalized = _EDITION_RE.sub(" ", normalized)

    # collapse any lingering leading articles after the above strips
    prev = None
    while prev != normalized:
        prev = normalized
        normalized = _LEADING_ARTICLE_RE.sub("", normalized).lstrip()

    normalized = _PUNCT_RE.sub(" ", normalized)
    normalized = _WS_RE.sub(" ", normalized).strip()

    return normalized or None
