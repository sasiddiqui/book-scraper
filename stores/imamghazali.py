import re
import time
from datetime import datetime, timezone
from html import unescape

import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper

PRODUCTS_API = "https://imamghazali.co/collections/store/products.json"
BOOK_PRODUCT_TYPES = {"Books", "Print Books", "Book", ""}
EXCLUDED_PRODUCT_TYPES = {"Poster"}
EXCLUDED_TAGS = {"Online Course", "IGP Membership Course"}
TITLE_EXCLUDE_RE = re.compile(
    r"qurbani|gift meals|waqf|endowment|online class",
    re.I,
)
AUTHOR_LABELS = {"author", "authors", "compiled by", "authored by"}
AUTHOR_LINE_RES = (
    re.compile(
        r"(?im)^(?:[\*\-•]\s*)?(?:\*\*)?Author(?:s)?(?:\*\*)?\s*[:\-]\s*(.+)$"
    ),
    re.compile(
        r"(?im)^(?:[\*\-•]\s*)?(?:\*\*)?Compiled by(?:\*\*)?\s*[:\-]\s*(.+)$"
    ),
    re.compile(
        r"(?im)^(?:[\*\-•]\s*)?(?:\*\*)?Authored by(?:\*\*)?\s*[:\-]\s*(.+)$"
    ),
    re.compile(r"(?im)^Authored by\s+(.+)$"),
)


class ImamGhazali(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://imamghazali.co/",
            "Imam Ghazali Publishing",
            convert_rate=1,
        )
        self.last_crawl_watermark: datetime | None = None

    def is_product_url(self, url: str) -> bool:
        return "/products/" in url and not url.endswith(".json")

    def _is_book_product(self, product: dict) -> bool:
        product_type = product.get("product_type") or ""
        if product_type in EXCLUDED_PRODUCT_TYPES:
            return False
        if product_type not in BOOK_PRODUCT_TYPES:
            return False

        tags = product.get("tags") or []
        if any(tag in EXCLUDED_TAGS for tag in tags):
            return False

        title = product.get("title") or ""
        if TITLE_EXCLUDE_RE.search(title):
            return False

        return True

    def _parse_shopify_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None

    def _normalize_crawl_watermark(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.astimezone().astimezone(timezone.utc)
        return value.astimezone(timezone.utc)

    def _fetch_catalog(self) -> list[dict]:
        products: list[dict] = []
        page = 1

        while True:
            response = requests.get(
                PRODUCTS_API,
                params={"limit": 250, "page": page},
                headers=self.headers,
                timeout=60,
            )
            response.raise_for_status()
            batch = response.json().get("products", [])
            if not batch:
                break

            products.extend(batch)
            page += 1

        return products

    def _collect_products(
        self, last_crawl_success: datetime | None
    ) -> tuple[list[dict], datetime | None]:
        all_products = [
            product
            for product in self._fetch_catalog()
            if self._is_book_product(product)
        ]
        watermark: datetime | None = None

        for product in all_products:
            updated = self._parse_shopify_datetime(product.get("updated_at"))
            if updated and (watermark is None or updated > watermark):
                watermark = updated

        last_success = self._normalize_crawl_watermark(last_crawl_success)
        products: list[dict] = []

        for product in all_products:
            if last_success:
                updated = self._parse_shopify_datetime(product.get("updated_at"))
                if updated and updated <= last_success:
                    continue

            products.append(product)

        if last_success:
            self.logger.info(
                "%s: %d/%d book products changed since last crawl",
                self.name,
                len(products),
                len(all_products),
            )
        else:
            self.logger.info(
                "%s: %d book products from catalog API", self.name, len(products)
            )

        return products, watermark

    def _strip_description(self, body_html: str | None) -> str | None:
        if not body_html:
            return None

        text = BeautifulSoup(body_html, "lxml").get_text(" ", strip=True)
        text = unescape(re.sub(r"\s+", " ", text)).strip()
        return text or None

    def _clean_author(self, value: str | None) -> str | None:
        if not value:
            return None

        # Join diacritics split across spans, e.g. "ibn ʿ Uqbah" -> "ibn ʿUqbah"
        author = unescape(re.sub(r"\s+", " ", value))
        author = re.sub(r"(?<=[ʿʾ'])\s+(?=\w)", "", author)
        author = author.strip(" .*,")
        author = re.split(
            r"\s*\|\s*|,\s*translated|\s+Forward by|\s+Foreword by",
            author,
            maxsplit=1,
            flags=re.I,
        )[0].strip(" .*,")

        if not author or author.lower() in AUTHOR_LABELS:
            return None
        if len(author) < 3 or len(author) > 160:
            return None
        return author

    def _author_from_body_html(self, body_html: str | None) -> str | None:
        if not body_html:
            return None

        soup = BeautifulSoup(body_html, "lxml")

        for li in soup.find_all("li"):
            text = li.get_text(" ", strip=True)
            match = re.match(
                r"(?i)^(Author|Authors|Compiled by|Authored by)\s*[:\-]\s*(.+)$",
                text,
            )
            if match:
                author = self._clean_author(match.group(2))
                if author:
                    return author

        for div in soup.find_all("div"):
            label = div.get_text(" ", strip=True).lower().rstrip(":")
            if label not in AUTHOR_LABELS or len(label) > 40:
                continue

            sibling = div.find_next_sibling("div")
            if sibling:
                author = self._clean_author(sibling.get_text(" ", strip=True))
                if author:
                    return author

            parent = div.parent
            if parent:
                kids = parent.find_all("div", recursive=False)
                for index, kid in enumerate(kids):
                    if kid is div and index + 1 < len(kids):
                        author = self._clean_author(
                            kids[index + 1].get_text(" ", strip=True)
                        )
                        if author:
                            return author

        text = soup.get_text("\n", strip=True)
        for pattern in AUTHOR_LINE_RES:
            match = pattern.search(text)
            if match:
                author = self._clean_author(match.group(1))
                if author:
                    return author

        # Edition-details style: "Author" on one line, name on the next
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        for index, line in enumerate(lines):
            if line.lower().rstrip(":") in AUTHOR_LABELS and index + 1 < len(lines):
                author = self._clean_author(lines[index + 1])
                if author:
                    return author

        # "Imam al-Nawawi, Foreword by ..."
        for line in lines:
            if ", Foreword by" in line or ", Forward by" in line:
                author = self._clean_author(line)
                if author:
                    return author

        return None

    def _product_core_info(self, product: dict) -> dict | None:
        variants = product.get("variants") or []
        if not variants:
            return None

        prices: list[float] = []
        for variant in variants:
            try:
                prices.append(float(variant["price"]))
            except (KeyError, TypeError, ValueError):
                continue
        if not prices:
            return None

        images = product.get("images") or []
        image = images[0]["src"] if images else None
        handle = product.get("handle")
        if not handle:
            return None

        book_info = {
            "url": f"{self.base_url}products/{handle}",
            "source": self.name,
            "title": product["title"],
            "price": min(prices),
            "instock": any(variant.get("available") for variant in variants),
            "publisher": product.get("vendor") or None,
            "description": self._strip_description(product.get("body_html")),
            "image": image,
        }

        author = self._author_from_body_html(product.get("body_html"))
        if author:
            book_info["author"] = author

        return book_info

    def extract_book_info(self, soup: BeautifulSoup, url: str) -> dict | None:
        # Catalog API is the primary path; this satisfies AbstractBookScraper.
        title_tag = soup.find("meta", property="og:title")
        if not title_tag or not title_tag.get("content"):
            return None

        book_info = {
            "url": url,
            "source": self.name,
            "title": unescape(title_tag["content"]).strip(),
        }

        author = self._author_from_body_html(str(soup))
        if author:
            book_info["author"] = author

        return book_info

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info("Crawling %s", self.name)
        start = time.time()

        self.test_base_url()

        raw_products, self.last_crawl_watermark = self._collect_products(
            last_crawl_success
        )
        if not raw_products:
            self.logger.warning("%s: no book products found in catalog API", self.name)
            return []

        for product in raw_products:
            book_info = self._product_core_info(product)
            if book_info is None:
                continue

            try:
                book = Book(**book_info)
                book.price *= self.convert_rate
            except ValidationError as error:
                self.logger.warning(
                    "Could not validate book info on %s: %s",
                    book_info.get("url"),
                    error,
                )
                continue

            self.add_book(book)
            self.logger.info(
                "SUCCESS - Added %s to all books - %s", book.title, book.url
            )

        self.logger.info(
            "Finished crawling %s in %.1fs — %d books collected",
            self.name,
            time.time() - start,
            len(self.all_books),
        )
        return self.all_books
