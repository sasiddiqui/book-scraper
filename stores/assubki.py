import requests
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper, ScraperError

CATALOGUE_URL = "https://assubkibooks.co.uk/catalogue.json"
# Frontend treats stored prices as USD and converts GBP at 0.75.
GBP_TO_USD = 1.33


class AsSubkiBooks(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://assubkibooks.co.uk/",
            "As-Subki Books",
            convert_rate=GBP_TO_USD,
        )

    def is_product_url(self, url: str) -> bool:
        return "/book/" in url

    def extract_book_info(self, soup, url) -> dict | None:
        return None

    def _to_book(self, item: dict) -> Book | None:
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        if not title or not url:
            return None

        try:
            price = float(item["price"])
        except (KeyError, TypeError, ValueError):
            return None

        book_info = {
            "title": title,
            "price": price,
            "url": url,
            "instock": bool(item.get("instock")),
            "source": self.name,
        }
        for field in ("author", "publisher", "image", "description"):
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                book_info[field] = " ".join(value.split())

        try:
            book = Book(**book_info)
        except ValidationError as error:
            self.logger.warning("Could not validate %s: %s", url, error)
            return None

        book.price = round(book.price * self.convert_rate, 2)
        return book

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info("Crawling %s", self.name)
        response = requests.get(CATALOGUE_URL, headers=self.headers, timeout=30)
        if response.status_code != 200:
            raise ScraperError(
                f"{self.name} scraper - Failed to fetch catalogue. "
                f"Status code: {response.status_code}."
            )

        for item in response.json().get("books") or []:
            book = self._to_book(item)
            if book is not None:
                self.add_book(book)

        self.logger.info("%s: %d books", self.name, len(self.all_books))
        return self.all_books
