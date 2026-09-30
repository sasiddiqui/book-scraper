"""
KEEP IN SYNC: CASES must be identical to CASES in the frontend repo's
`src/lib/server/search/normalize.test.ts`. The two normalizers have to produce
byte-identical output, or stored `*Normalized` fields drift from query text.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from normalize import compute_updates, normalize_text, sanitize_arabic_text  # noqa: E402

CASES = [
    # hamza / alif forms
    ("أحمد", "احمد"),
    ("إسلام", "اسلام"),
    ("آمن", "امن"),
    ("ٱلرحمن", "رحمن"),
    ("مؤمن", "مومن"),
    ("مسائل", "مسايل"),
    # ة/ه, ى/ي, Persian letters, tatweel
    ("مكتبة", "مكتبه"),
    ("موسى", "موسي"),
    ("علی", "علي"),
    ("کتاب", "كتاب"),
    ("كتـــاب", "كتاب"),
    # harakat
    ("بِسْمِ اللَّهِ", "بسم الله"),
    # leading ال
    ("الله", "الله"),
    ("ﷲ", "الله"),
    ("البخاري", "بخاري"),
    ("(البخاري)", "بخاري"),
    ("صحيح البخاري", "صحيح بخاري"),
    ("الكتاب", "كتاب"),
    ("الحج", "الحج"),
    ("الإمام", "امام"),
    ("السلام عليكم", "سلام عليكم"),
    # Latin diacritics and transliteration marks
    ("Ṣaḥīḥ al-Bukhārī", "sahih al bukhari"),
    ("Qurʾān", "qur an"),
    ("Qur'an", "qur an"),
    ("Qur’an", "qur an"),
    ("Riyāḍ al-Ṣāliḥīn", "riyad al salihin"),
    ("İstanbul", "istanbul"),
    ("Şeker", "seker"),
    # punctuation, width, whitespace
    ("Al-Bukhari", "al bukhari"),
    ("Dar-us-Salam (Vol. 2)", "dar us salam vol 2"),
    ("ＡＢＣ", "abc"),
    ("  Hello   World  ", "hello world"),
    ("a(b)*c+?", "a b c"),
    ("", ""),
]


@pytest.mark.parametrize("text, expected", CASES)
def test_normalize_text(text, expected):
    assert normalize_text(text) == expected


def test_case_table_is_idempotent():
    for _, expected in CASES:
        assert normalize_text(expected) == expected


def test_al_handling():
    assert normalize_text("الله") == "الله"
    assert normalize_text("البخاري") == "بخاري"
    assert normalize_text("(البخاري)") == "بخاري"


def test_sanitize_arabic_text_passes_through_empty_values():
    assert sanitize_arabic_text(None) is None
    assert sanitize_arabic_text("") == ""
    assert sanitize_arabic_text("Ṣaḥīḥ al-Bukhārī") == "sahih al bukhari"


def test_whitespace_only_input():
    assert normalize_text("   \t\n ") == ""


def test_compute_updates_fills_and_fixes_fields():
    doc = {
        "title": "Ṣaḥīḥ al-Bukhārī",
        "author": "Imam al-Bukhari",
        "authorArabic": "الإمام البخاري",
        "publisher": "Dar al-Salam",
        "titleNormalized": "Ṣaḥīḥ al-Bukhārī",  # old style: unfolded
        "authorNormalized": "imam al bukhari",  # already current
    }
    assert compute_updates(doc) == {
        "titleNormalized": "sahih al bukhari",
        "authorArabicNormalized": "امام بخاري",
        "publisherNormalized": "dar al salam",
    }


def test_compute_updates_is_idempotent_and_skips_current_rows():
    doc = {"title": "Riyad al-Salihin", "author": None, "publisher": ""}
    doc.update(compute_updates(doc))
    assert doc["titleNormalized"] == "riyad al salihin"
    assert compute_updates(doc) == {}


def test_compute_updates_treats_missing_optional_fields_as_none():
    # absent normalized field and absent raw field: nothing to write
    assert compute_updates({"title": "Fiqh", "titleNormalized": "fiqh"}) == {}
    # raw field gone but a stale normalized value remains: clear it
    assert compute_updates(
        {"title": "Fiqh", "titleNormalized": "fiqh", "authorNormalized": "stale"}
    ) == {"authorNormalized": None}
