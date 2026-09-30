from bs4 import BeautifulSoup

from stores.kastntinya import Kastntinya


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _page(title_html: str, extra_head: str = "") -> str:
    return f"""
    <html>
      <head>
        {extra_head}
        <meta property="product:availability" content="in stock">
        <meta property="og:image" content="https://example.com/cover.jpg">
      </head>
      <body>
        {title_html}
        <h2 class="product-formatted-price theme-text-primary">40.00</h2>
      </body>
    </html>
    """


def test_arabic_name_then_english_seo_title():
    html = _page(
        "<h1>بدائع التفسير 3 مجلدات - ابن القيم - دار ابن الجوزي</h1>",
        """
        <meta property="og:title" content="Bada'' al-Tafsir 3 Majaladat - Ibn al-Qayyim - Dar Ibn al-Ja">
        <meta name="description" content="Bada'' al-Tafsir 3 Majaladat - Ibn al-Qayyim - Dar Ibn al-Jawzi">
        """,
    )
    book = Kastntinya().extract_book_info(_soup(html), "https://kastntinya.com/ar-us/products/example")

    assert book["title"] == "بدائع التفسير 3 مجلدات\nBada'' al-Tafsir 3 Majaladat"
    assert book["author"] == "ابن القيم"
    assert book["publisher"] == "دار ابن الجوزي"


def test_arabic_seo_title_is_not_appended():
    html = _page(
        "<h1>النحو الصغير | سليمان العيوني | ط مكتبة الرشد</h1>",
        '<meta property="og:title" content="النحو الصغير | سليمان العيوني | ط مكتبة الرشد">',
    )
    book = Kastntinya().extract_book_info(_soup(html), "https://kastntinya.com/ar-us/products/example")

    assert book["title"] == "النحو الصغير"
    assert book["author"] == "سليمان العيوني"
    assert book["publisher"] == "ط مكتبة الرشد"


def test_truncated_english_title_uses_the_longer_description():
    html = _page(
        "<h1>معارج القبول بشرح سلم الوصول إلى علم الاصول 1/3 | ط دار ابن الجوزي</h1>",
        """
        <meta property="og:title" content="M'araj al-Qabwl Basaharah Salam al-Wswl ila Ilm al-Aswl 1/3 ">
        <meta name="description" content="M'araj al-Qabwl Basaharah Salam al-Wswl ila Ilm al-Aswl 1/3 | Ed. Dar Ibn al-Jawzi">
        """,
    )
    book = Kastntinya().extract_book_info(_soup(html), "https://kastntinya.com/ar-us/products/example")

    assert book["title"] == (
        "معارج القبول بشرح سلم الوصول إلى علم الاصول 1/3\n"
        "M'araj al-Qabwl Basaharah Salam al-Wswl ila Ilm al-Aswl 1/3"
    )
    assert "author" not in book
    assert "publisher" not in book
