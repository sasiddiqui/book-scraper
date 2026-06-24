import re

from bs4 import BeautifulSoup

from stores.woostorescraper import WooStoreScraper

LIBRARY_CATEGORY_ID = 8516


class SafinatUlNajat(WooStoreScraper):
    def __init__(self):
        super().__init__(
            "Safinat Ul-Najat",
            "https://safinatulnajat.com",
            category_ids=[LIBRARY_CATEGORY_ID],
            convert_rate=1.33,
        )

    def _attribute_terms(self, product: dict, taxonomy: str) -> str | None:
        for attribute in product.get("attributes", []):
            if attribute.get("taxonomy") != taxonomy:
                continue
            terms = [term["name"] for term in attribute.get("terms", []) if term.get("name")]
            if terms:
                return ", ".join(terms)
        return None

    def _parse_short_description(self, html: str | None) -> tuple[str | None, str | None]:
        if not html:
            return None, None

        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text("\n", strip=True)

        author = None
        publisher = None

        author_match = re.search(
            r"(?:Author|اسم المؤلف|المؤلف)\s*[:：]\s*(.+)",
            text,
            re.IGNORECASE,
        )
        if author_match:
            author = author_match.group(1).strip()

        publisher_match = re.search(
            r"(?:Publisher|الناشر)\s*[:：]\s*(.+)",
            text,
            re.IGNORECASE,
        )
        if publisher_match:
            publisher = publisher_match.group(1).strip()

        return author, publisher

    def extract_book_info(self, product: dict) -> dict | None:
        book_info = super().extract_book_info(product)
        if book_info is None:
            return None

        author = self._attribute_terms(product, "pa_book-author")
        publisher = self._attribute_terms(product, "pa_publisher")

        if not publisher and product.get("brands"):
            publisher = product["brands"][0].get("name")

        html_author, html_publisher = self._parse_short_description(
            product.get("short_description")
        )
        author = author or html_author
        publisher = publisher or html_publisher

        book_info["author"] = author
        book_info["publisher"] = publisher
        book_info["description"] = product.get("short_description") or product.get(
            "description"
        )

        return book_info
