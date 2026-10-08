import os
import time
from datetime import datetime, timezone
from html import unescape

import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper, ScraperError

# Public Storefront token from the theme. It can read author metaobjects.
# An Admin token is not required. Override with MAWLANA_TOKEN if the theme token rotates.
STOREFRONT_TOKEN = "a9f95826a8a9ca93a4a37e89729ad82b"
API_VERSION = "2025-01"
PAGE_SIZE = 250

PRODUCTS_QUERY = """
query GetProducts($cursor: String) {
  products(first: %d, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        title
        handle
        onlineStoreUrl
        description
        updatedAt
        availableForSale
        featuredImage {
          url(transform: { maxWidth: 800, preferredContentType: JPG })
        }
        priceRange { minVariantPrice { amount } }
        author: metafield(namespace: "custom", key: "author_reference") {
          references(first: 8) {
            nodes {
              ... on Metaobject {
                name: field(key: "name") { value }
              }
            }
          }
        }
      }
    }
  }
}
""" % PAGE_SIZE


class MawlanaBooks(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://mawlanabooks.com/",
            "Mawlana Books",
            convert_rate=1,
        )
        self.last_crawl_watermark: datetime | None = None
        token = os.getenv("MAWLANA_TOKEN") or STOREFRONT_TOKEN
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "X-Shopify-Storefront-Access-Token": token,
                "User-Agent": self.headers["User-Agent"],
            }
        )

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

    def _graphql(self, cursor: str | None) -> dict:
        response = self.session.post(
            f"{self.base_url}api/{API_VERSION}/graphql.json",
            json={"query": PRODUCTS_QUERY, "variables": {"cursor": cursor}},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors") and not payload.get("data"):
            raise ScraperError(
                f"{self.name} scraper - Storefront query failed: {payload['errors'][0].get('message')}"
            )
        products = (payload.get("data") or {}).get("products")
        if not products:
            raise ScraperError(f"{self.name} scraper - Storefront query returned no products")
        return products

    def _fetch_catalog(self) -> list[dict]:
        products: list[dict] = []
        cursor = None

        while True:
            page = self._graphql(cursor)
            products.extend(edge["node"] for edge in page["edges"])
            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]

        return products

    def _collect_products(
        self, last_crawl_success: datetime | None
    ) -> tuple[list[dict], datetime | None]:
        all_products = self._fetch_catalog()
        watermark: datetime | None = None

        for product in all_products:
            updated = self._parse_shopify_datetime(product.get("updatedAt"))
            if updated and (watermark is None or updated > watermark):
                watermark = updated

        last_success = self._normalize_crawl_watermark(last_crawl_success)
        products: list[dict] = []

        for product in all_products:
            if last_success:
                updated = self._parse_shopify_datetime(product.get("updatedAt"))
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
            self.logger.info(
                "%s: %d products from Storefront API", self.name, len(products)
            )

        return products, watermark

    def _image_url(self, image: dict | None) -> str | None:
        if not image:
            return None
        url = (image.get("url") or "").strip()
        if url.startswith("//"):
            url = "https:" + url
        return url or None

    def _author(self, product: dict) -> str | None:
        nodes = (((product.get("author") or {}).get("references") or {}).get("nodes")) or []
        names: list[str] = []
        for node in nodes:
            name = ((node.get("name") or {}).get("value") or "").strip()
            if name and name not in names:
                names.append(name)
        return ", ".join(names) or None

    def _product_info(self, product: dict) -> dict | None:
        title = (product.get("title") or "").strip()
        handle = product.get("handle")
        if not title or not handle:
            return None

        try:
            price = float(product["priceRange"]["minVariantPrice"]["amount"])
        except (KeyError, TypeError, ValueError):
            return None

        description = unescape(product.get("description") or "")
        description = " ".join(description.split()) or None
        url = product.get("onlineStoreUrl") or f"{self.base_url}products/{handle}"

        book_info = {
            "url": url,
            "source": self.name,
            "title": title,
            "price": price,
            "instock": bool(product.get("availableForSale")),
            "description": description,
            "image": self._image_url(product.get("featuredImage")),
        }
        author = self._author(product)
        if author:
            book_info["author"] = author
        return book_info

    def extract_book_info(self, soup: BeautifulSoup, url: str) -> dict | None:
        # Storefront catalog is the primary path; this satisfies AbstractBookScraper.
        title_tag = soup.find("meta", property="og:title")
        if not title_tag or not title_tag.get("content"):
            return None
        return {
            "url": url,
            "source": self.name,
            "title": unescape(title_tag["content"]).strip(),
        }

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info("Crawling %s", self.name)
        start = time.time()

        self.test_base_url()

        raw_products, self.last_crawl_watermark = self._collect_products(
            last_crawl_success
        )
        if not raw_products:
            self.logger.warning("%s: no products found in Storefront API", self.name)
            return []

        for product in raw_products:
            book_info = self._product_info(product)
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
