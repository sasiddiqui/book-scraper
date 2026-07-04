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

PRODUCTS_API = "https://www.whitethreadpress.com/collections/all/products.json"
AUTHOR_TITLE_RE = re.compile(r'title="Explore all books authored by ([^"]+)"')
PUBLISHER_TITLE_RE = re.compile(
    r'title="Bestselling Islamic Books Published by ([^"]+)"'
)
NON_AUTHOR_TAGS = {
    "Arbain Series",
    "Beliefs",
    "Biography - Seerah",
    "Bundles",
    "Contemporary",
    "Dar al Qurra",
    "E-Book",
    "eBook Format",
    "Hadiths",
    "Hardback",
    "Hardcover with Jacket",
    "History",
    "Law",
    "Prayer (Du'a)",
    "Qur'an",
    "Repentance",
    "Softcover",
    "Spirituality",
    "Travel",
    "Turath Publishing",
    "White Thread Press",
}


class WhiteThreadPress(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://www.whitethreadpress.com/",
            "White Thread Press",
            convert_rate=1,
        )
        self.batch_size = 5
        self.batch_delay = 0.5
        self.last_crawl_watermark: datetime | None = None

    def is_product_url(self, url: str) -> bool:
        return "/products/" in url and not url.endswith(".json")

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
        all_products = self._fetch_catalog()
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
                "%s: %d/%d products changed since last crawl",
                self.name,
                len(products),
                len(all_products),
            )
        else:
            self.logger.info("%s: %d products from catalog API", self.name, len(products))

        return products, watermark

    def _strip_description(self, body_html: str | None) -> str | None:
        if not body_html:
            return None

        text = BeautifulSoup(body_html, "lxml").get_text(" ", strip=True)
        text = unescape(re.sub(r"\s+", " ", text)).strip()
        return text or None

    def _author_from_tags(self, tags: list[str] | None) -> str | None:
        if not tags:
            return None

        authors = [tag.strip() for tag in tags if tag.strip() not in NON_AUTHOR_TAGS]
        if not authors:
            return None

        return ", ".join(authors)

    def _extract_table_values(self, html: str, label: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        values: list[str] = []

        for row in soup.select("table tr"):
            header = row.find("th")
            cell = row.find("td")
            if not header or not cell:
                continue
            if header.get_text(strip=True).lower() != label.lower():
                continue

            links = cell.find_all("a")
            if links:
                values.extend(link.get_text(strip=True) for link in links if link.get_text(strip=True))
            else:
                text = cell.get_text(" ", strip=True)
                if text:
                    values.append(text)

        return values

    def _extract_author_from_html(self, html: str, tags: list[str] | None = None) -> str | None:
        match = AUTHOR_TITLE_RE.search(html)
        if match:
            return match.group(1).strip()

        table_authors = self._extract_table_values(html, "Book Authors")
        if table_authors:
            return ", ".join(table_authors)

        return self._author_from_tags(tags)

    def _extract_publisher_from_html(self, html: str, fallback: str | None) -> str | None:
        match = PUBLISHER_TITLE_RE.search(html)
        if match:
            return match.group(1).strip()

        table_publishers = self._extract_table_values(html, "Book Publishers")
        if table_publishers:
            return table_publishers[0]

        publisher_link = BeautifulSoup(html, "lxml").select_one(".productpagepublisher a")
        if publisher_link:
            text = publisher_link.get_text(" ", strip=True)
            if text.lower().startswith("publisher:"):
                text = text.split(":", 1)[1].strip()
            if text:
                return text

        return fallback

    def _product_core_info(self, product: dict) -> dict | None:
        variants = product.get("variants") or []
        if not variants:
            return None

        available_variants = [variant for variant in variants if variant.get("available")]
        variant = available_variants[0] if available_variants else variants[0]

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
            "instock": any(variant.get("available") for variant in variants),
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
            self.logger.warning("%s: no products found in catalog API", self.name)
            return []

        pending: list[dict] = []
        for product in raw_products:
            core = self._product_core_info(product)
            if core is None:
                continue
            pending.append(
                {
                    "core": core,
                    "tags": product.get("tags") or [],
                }
            )

        if not pending:
            self.logger.warning("%s: no valid products after parsing", self.name)
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
                    tags = item.get("tags")

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
