import asyncio
import re
import time
from datetime import datetime, timezone
from html import unescape

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper

PRODUCTS_API = "https://turath.co.uk/collections/all/products.json"
AUTHOR_TITLE_RES = (
    re.compile(r'title="View all books authored by ([^"]+)"'),
    re.compile(r'title="View all books translated by ([^"]+)"'),
    re.compile(r'title="View all books compiled by ([^"]+)"'),
    re.compile(r'title="View all books edited by ([^"]+)"'),
)
PUBLISHER_TITLE_RE = re.compile(r'title="View all Books Published by ([^"]+)"')
EXCLUDED_PRODUCT_TYPES = {"On-Demand Course"}
CATEGORY_TAGS = {
    "Softcover",
    "E-Book",
    "eBook Format",
    "Hardback",
    "Paperback",
    "Spiral Book",
    "Turath Publishing",
    "White Thread Press",
    "Ibn Daud Books",
    "Bundles",
    "Qur'an-Bundle",
    "Du'a-Book-Bundle",
    "Gift Edition",
    "Contemporary",
    "Spirituality",
    "Hadiths",
    "History",
    "Beliefs",
    "Law",
    "Prayer (Du'a)",
    "Qur'an",
    "Arbain Series",
    "Biography - Seerah",
    "Fiqh",
    "Tafsir",
    "Seerah",
    "Ramadan",
    "New Arrival",
    "Sale",
    "Featured",
    "Print Books",
    "Children",
    "Bundle",
    "Courses",
    "Travel",
    "Earliest Sirah",
    "Historiography",
    "Islamic History",
    "Life of the Prophet",
    "Maghazi",
    "Seerah Book",
    "Huma Press",
    "Jāmiʿ at-Tirmidhī Series",
    "Mufti Muhammad Taqi Usmani Hadiths",
    "Global Bookshelves",
}


class TurathPublishing(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://turath.co.uk/",
            "Turath Publishing",
            convert_rate=1,
        )
        self.batch_size = 5
        self.batch_delay = 0.5
        self.last_crawl_watermark: datetime | None = None

    def is_product_url(self, url: str) -> bool:
        return "/products/" in url and not url.endswith(".json")

    def _is_book_product(self, product: dict) -> bool:
        return product.get("product_type") not in EXCLUDED_PRODUCT_TYPES

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

    def _author_from_tags(self, tags: list[str] | None) -> str | None:
        for tag in tags or []:
            if tag in CATEGORY_TAGS:
                continue
            if tag.endswith(" Series") or tag.endswith("-Bundle"):
                continue
            return tag
        return None

    def _extract_author_from_html(
        self, html: str, tags: list[str] | None = None
    ) -> str | None:
        for pattern in AUTHOR_TITLE_RES:
            match = pattern.search(html)
            if match:
                return match.group(1).strip()
        return self._author_from_tags(tags)

    def _extract_publisher_from_html(self, html: str, fallback: str | None) -> str | None:
        match = PUBLISHER_TITLE_RE.search(html)
        if match:
            return match.group(1).strip()
        return fallback

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
        price = min(prices)

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
            "instock": any(v.get("available") for v in variants),
            "publisher": product.get("vendor") or None,
            "description": self._strip_description(product.get("body_html")),
            "image": image,
        }

    def extract_book_info(self, soup: BeautifulSoup, url: str) -> dict | None:
        html = str(soup)
        book_info = {
            "url": url,
            "source": self.name,
        }

        title_tag = soup.find("meta", property="og:title")
        if title_tag and title_tag.get("content"):
            book_info["title"] = unescape(title_tag["content"]).strip()
        else:
            return None

        author = self._extract_author_from_html(html)
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

        raw_products, self.last_crawl_watermark = self._collect_products(
            last_crawl_success
        )
        if not raw_products:
            self.logger.warning("%s: no book products found in catalog API", self.name)
            return []

        pending: list[dict] = []
        for product in raw_products:
            core = self._product_core_info(product)
            if core is None:
                continue
            pending.append({"core": core, "tags": product.get("tags") or []})

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
                    tags = item.get("tags") or []

                    if isinstance(result, Exception):
                        self.logger.error(
                            "Exception while fetching %s: %s", core["url"], result
                        )
                        continue

                    _, response = result
                    author = self._author_from_tags(tags)
                    publisher = core.get("publisher")
                    if response:
                        html = response.decode("utf-8", errors="replace")
                        author = self._extract_author_from_html(html, tags)
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
