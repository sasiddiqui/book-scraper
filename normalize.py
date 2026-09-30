"""
Text normalization for the searchable `*Normalized` catalog fields.

This is a line-for-line Python port of `normalizeText` in the frontend repo
(`Islamic-Book-Compare/src/lib/server/search/normalize.ts`). The two MUST stay
in sync: the frontend compares query tokens against these stored values as
plain substrings, so any drift silently breaks matching. The shared case table
lives in `tests/test_normalize.py` (Python) and `normalize.test.ts` (TS).

Kept free of any DB / network imports on purpose so it can be unit tested (and
imported by `upload.py`, which connects to Mongo at import time).

Steps, in order:
    1. NFKC
    2. fold Latin diacritics (NFD, drop combining marks, NFC)
    3. fold Arabic letter variants
    4. drop harakat and tatweel
    5. lowercase
    6. everything that is not a letter/digit becomes a space
    7. strip a leading ال from each word when >= 3 letters remain
    8. collapse whitespace and trim
"""

from __future__ import annotations

import re
import unicodedata

# Step 3: أ إ آ ٱ -> ا, ؤ -> و, ئ ى ی -> ي, ة -> ه, ک -> ك
_ARABIC_FOLDS = str.maketrans(
    {
        "\u0623": "\u0627",  # أ -> ا
        "\u0625": "\u0627",  # إ -> ا
        "\u0622": "\u0627",  # آ -> ا
        "\u0671": "\u0627",  # ٱ -> ا
        "\u0624": "\u0648",  # ؤ -> و
        "\u0626": "\u064A",  # ئ -> ي
        "\u0649": "\u064A",  # ى -> ي
        "\u06CC": "\u064A",  # ی (Persian/Urdu yeh) -> ي
        "\u0629": "\u0647",  # ة -> ه
        "\u06A9": "\u0643",  # ک (Persian/Urdu kaf) -> ك
    }
)

# Step 4: harakat / Quranic marks and tatweel.
_HARAKAT_TATWEEL_RE = re.compile("[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED\u0640]")

_AL = "\u0627\u0644"  # ال
_MIN_LETTERS_AFTER_AL = 3


def _is_word_char(ch: str) -> bool:
    """Letter or digit (Unicode L*/N*), excluding the U+02B0-U+02FF modifier block.

    That block holds apostrophe-like letters (ʿ ʾ ʼ ʻ) that are category Lm but
    must act as separators, exactly like the frontend's step 6.
    """
    if "\u02b0" <= ch <= "\u02ff":
        return False
    return unicodedata.category(ch)[0] in ("L", "N")


def normalize_text(text: str) -> str:
    """Normalize `text` for search. Always returns a string ('' for empty input)."""
    if not text:
        return ""

    # 1-2. NFKC, then remove every combining mark (this also splits and drops the
    # hamza/madda marks of أ إ آ ؤ ئ, which step 3 folds explicitly anyway).
    s = unicodedata.normalize("NFKC", text)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c)[0] != "M")
    s = unicodedata.normalize("NFC", s)

    # 3.
    s = s.translate(_ARABIC_FOLDS)

    # 4.
    s = _HARAKAT_TATWEEL_RE.sub("", s)

    # 5.
    s = s.lower()

    # 6. (whitespace is not a word char, so it also becomes a space)
    s = "".join(c if _is_word_char(c) else " " for c in s)

    # 7-8.
    words = []
    for word in s.split(" "):
        if not word:
            continue
        if word.startswith(_AL) and len(word) - len(_AL) >= _MIN_LETTERS_AFTER_AL:
            word = word[len(_AL) :]
        words.append(word)
    return " ".join(words)


def sanitize_arabic_text(text: str | None) -> str | None:
    """Catalog entry point (kept under its historical name for `enrich_authors.py`).

    `None` and '' pass through unchanged so absent fields stay absent.
    """
    if not text:
        return text
    return normalize_text(text)


# normalized field -> raw field it is derived from (mirrors `upload.upload_books`)
FIELD_SOURCES = {
    "titleNormalized": "title",
    "authorNormalized": "author",
    "authorArabicNormalized": "authorArabic",
    "publisherNormalized": "publisher",
}


def compute_updates(doc: dict) -> dict:
    """Normalized fields of `doc` that differ from what the current normalizer produces.

    Same rules as `upload.upload_books`: a normalized field is `None` when its raw
    field is missing or empty; `titleNormalized` alone passes a falsy title through
    unchanged. Returns only the fields that need a write (empty dict = up to date).
    """
    changes = {}
    for target, source in FIELD_SOURCES.items():
        raw = doc.get(source)
        if raw and isinstance(raw, str):
            expected = normalize_text(raw)
        elif target == "titleNormalized":
            expected = raw
        else:
            expected = None
        if doc.get(target) != expected:
            changes[target] = expected
    return changes
