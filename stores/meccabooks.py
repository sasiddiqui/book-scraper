import asyncio
import re
import time
from datetime import datetime
from html import unescape
from urllib.parse import unquote

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper

PRODUCTS_API = "https://www.meccabooks.com/collections/all/products.json"
BOOK_PRODUCT_TYPES = {
    "Book",
    "Hardcover",
    "Quality Paper",
    "Children's Book",
    "Paperback",
    "Softcover",
}
AUTHOR_FILTER_RE = re.compile(r'filter\.p\.m\.custom\.author=([^"&]+)')
PUBLISHER_FILTER_RE = re.compile(r'filter\.p\.m\.custom\.publisher=([^"&]+)')
OG_TITLE_RE = re.compile(r'<meta property="og:title" content="([^"]+)"')
ABOUT_AUTHOR_NAME_RE = re.compile(
    r"^(.+?)\s+(?:was|is|graduated|has published|has written|has)\b",
    re.I,
)


class MeccaBooks(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://www.meccabooks.com/",
            "Mecca Books",
            convert_rate=1,
        )
        self.batch_size = 5
        self.batch_delay = 0.5

    def is_product_url(self, url: str) -> bool:
        return "/products/" in url and not url.endswith(".json")

    def _is_book_product(self, product: dict) -> bool:
        if product.get("product_type") in BOOK_PRODUCT_TYPES:
            return True
        tags = product.get("tags") or []
        return any(tag == "cat:books" or tag.startswith("cat:books") for tag in tags)

    def _parse_shopify_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            return None

    def _collect_products(self, last_crawl_success: datetime | None) -> list[dict]:
        products: list[dict] = []
        page = 1
        last_success = last_crawl_success
        if last_success and getattr(last_success, "tzinfo", None):
            last_success = last_success.replace(tzinfo=None)

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

            for product in batch:
                if not self._is_book_product(product):
                    continue

                if last_success:
                    updated = self._parse_shopify_datetime(product.get("updated_at"))
                    if updated and updated < last_success:
                        continue

                products.append(product)

            page += 1

        self.logger.info("%s: %d book products from catalog API", self.name, len(products))
        return products

    def _strip_description(self, body_html: str | None) -> str | None:
        if not body_html:
            return None

        about_match = re.search(r"About The Author", body_html, re.I)
        if about_match:
            body_html = body_html[: about_match.start()]

        text = BeautifulSoup(body_html, "lxml").get_text(" ", strip=True)
        text = unescape(re.sub(r"\s+", " ", text)).strip()
        return text or None

    def _author_from_body_html(self, body_html: str | None) -> str | None:
        if not body_html:
            return None

        match = re.search(r"About The Author", body_html, re.I)
        if not match:
            return None

        snippet = body_html[match.end() : match.end() + 800]
        text = BeautifulSoup(snippet, "lxml").get_text(" ", strip=True)
        text = unescape(re.sub(r"\s+", " ", text)).strip()
        if not text:
            return None

        name_match = ABOUT_AUTHOR_NAME_RE.match(text)
        if not name_match:
            return None

        author = name_match.group(1).strip(" .,-")
        return author or None

    def _extract_author_from_html(
        self, html: str, body_html: str | None = None
    ) -> str | None:
        match = AUTHOR_FILTER_RE.search(html)
        if match:
            return unquote(match.group(1)).strip()

        og_match = OG_TITLE_RE.search(html)
        if og_match:
            title = unescape(og_match.group(1)).strip()
            if " by " in title:
                return title.rsplit(" by ", 1)[1].strip()

        return self._author_from_body_html(body_html)

    def _extract_publisher_from_html(self, html: str, fallback: str | None) -> str | None:
        match = PUBLISHER_FILTER_RE.search(html)
        if match:
            return unquote(match.group(1)).strip()
        return fallback

    def _product_core_info(self, product: dict) -> dict | None:
        variants = product.get("variants") or []
        if not variants:
            return None

        variant = variants[0]
        try:
            price = float(variant["price"])
        except (KeyError, TypeError, ValueError):
            return None

        images = product.get("images") or []
        image = images[0]["src"] if images else None
        handle = product.get("handle")
        if not handle:
            return None

        return {
            "url": f"{self.base_url}products/{handle}",
            "source": self.name,
            "title": product["title"],
            "price": price,
            "instock": bool(variant.get("available")),
            "publisher": product.get("vendor") or None,
            "description": self._strip_description(product.get("body_html")),
            "image": image,
        }

    def extract_book_info(self, soup: BeautifulSoup, url: str) -> dict | None:
        html = str(soup)
        body_html = None
        about = soup.find(string=re.compile(r"About The Book", re.I))
        if about:
            body_html = html

        book_info = {
            "url": url,
            "source": self.name,
        }

        title_tag = soup.find("meta", property="og:title")
        if title_tag and title_tag.get("content"):
            title = unescape(title_tag["content"]).strip()
            if " by " in title:
                book_info["author"] = title.rsplit(" by ", 1)[1].strip()
                title = title.rsplit(" by ", 1)[0].strip()
            book_info["title"] = title
        else:
            return None

        author = self._extract_author_from_html(html, body_html)
        if author:
            book_info["author"] = author

        publisher = self._extract_publisher_from_html(html, None)
        if publisher:
            book_info["publisher"] = publisher

        return book_info

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info("Crawling %s", self.name)
        start = time.time()

        self.test_base_url()

        raw_products = self._collect_products(last_crawl_success)
        if not raw_products:
            self.logger.warning("%s: no book products found in catalog API", self.name)
            return []

        pending: list[dict] = []
        for product in raw_products:
            core = self._product_core_info(product)
            if core is None:
                continue
            pending.append(
                {
                    "core": core,
                    "body_html": product.get("body_html"),
                }
            )

        if not pending:
            self.logger.warning("%s: no valid book products after parsing", self.name)
            return []

        async with aiohttp.ClientSession() as session:
            while pending:
                batch = pending[: self.batch_size]
                del pending[: self.batch_size]

                tasks = [
                    asyncio.create_task(self.fetch_page(session, item["core"]["url"]))
                    for item in batch
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for item, result in zip(batch, responses):
                    core = item["core"]
                    body_html = item.get("body_html")

                    if isinstance(result, Exception):
                        self.logger.error(
                            "Exception while fetching %s: %s", core["url"], result
                        )
                        continue

                    _, response = result
                    author = None
                    publisher = core.get("publisher")
                    if response:
                        html = response.decode("utf-8", errors="replace")
                        author = self._extract_author_from_html(html, body_html)
                        publisher = self._extract_publisher_from_html(
                            html, core.get("publisher")
                        )

                    book_info = {
                        key: value
                        for key, value in core.items()
                        if not key.startswith("_")
                    }
                    if author:
                        book_info["author"] = author
                    if publisher:
                        book_info["publisher"] = publisher

                    try:
                        book = Book(**book_info)
                        book.price *= self.convert_rate
                    except ValidationError as error:
                        self.logger.warning(
                            "Could not validate book info on %s: %s",
                            core["url"],
                            error,
                        )
                        continue

                    self.add_book(book)
                    self.logger.info(
                        "SUCCESS - Added %s to all books - %s", book.title, core["url"]
                    )

                if pending and self.batch_delay > 0:
                    await asyncio.sleep(self.batch_delay)

        self.logger.info(
            "Finished crawling %s in %.1fs — %d books collected",
            self.name,
            time.time() - start,
            len(self.all_books),
        )
        return self.all_books
