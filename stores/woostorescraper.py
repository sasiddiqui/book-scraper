# scraper for woo store API based websites (public, no auth)

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

from scraper import ScraperError

logger = logging.getLogger("scraper")


class WooStoreScraper:
    PER_PAGE = 100

    def __init__(
        self,
        name: str,
        base_url: str,
        category_ids: list[int] | None = None,
        convert_rate: float = 1,
        page_delay: float = 0.1,
    ):
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.category_ids = category_ids
        self.convert_rate = convert_rate
        self.page_delay = page_delay
        self.store_api = f"{self.base_url}/wp-json/wc/store/v1"
        self._allowed_category_ids: set[int] | None = None

    def _get_allowed_category_ids(self) -> set[int]:
        if self._allowed_category_ids is not None:
            return self._allowed_category_ids

        if not self.category_ids:
            self._allowed_category_ids = set()
            return self._allowed_category_ids

        root_id = self.category_ids[0]
        all_categories: list[dict] = []
        page = 1

        while True:
            response = requests.get(
                f"{self.store_api}/products/categories",
                params={"per_page": self.PER_PAGE, "page": page},
                timeout=60,
            )
            if response.status_code != 200:
                raise ScraperError(
                    f"{self.name} scraper - Failed to load product categories. "
                    f"Status code: {response.status_code}"
                )

            batch = response.json()
            if not batch:
                break

            all_categories.extend(batch)
            total_pages = int(response.headers.get("X-WP-TotalPages", page))
            if page >= total_pages:
                break
            page += 1

        allowed = {root_id}
        changed = True
        while changed:
            changed = False
            for category in all_categories:
                cat_id = category.get("id")
                parent_id = category.get("parent")
                if cat_id not in allowed and parent_id in allowed:
                    allowed.add(cat_id)
                    changed = True

        self._allowed_category_ids = allowed
        return self._allowed_category_ids

    def test_base_url(self):
        response = requests.get(
            f"{self.store_api}/products",
            params={"per_page": 1},
            timeout=30,
        )
        if response.status_code == 200:
            return True
        raise ScraperError(
            f"{self.name} scraper - Failed to reach {self.base_url}. "
            f"Status code: {response.status_code}. Skipping scraper..."
        )

    def _minor_to_major(self, amount: str | int, minor_unit: int) -> float:
        return int(amount) / (10 ** minor_unit)

    def _product_price(self, product: dict) -> float | None:
        prices = product.get("prices") or {}
        minor_unit = prices.get("currency_minor_unit", 2)

        if product.get("on_sale") and prices.get("sale_price"):
            raw = prices["sale_price"]
        else:
            raw = prices.get("price")

        if raw is None:
            return None

        return self._minor_to_major(raw, minor_unit)

    def is_target_product(self, product: dict) -> bool:
        if not self.category_ids:
            return True

        allowed_ids = self._get_allowed_category_ids()
        for category in product.get("categories", []):
            if category.get("id") in allowed_ids:
                return True
        return False

    def extract_book_info(self, product: dict) -> dict | None:
        price = self._product_price(product)
        if price is None:
            logger.warning(f"Could not find price for {product.get('permalink')}")
            return None

        images = product.get("images") or []
        book_info = {
            "source": self.name,
            "url": product["permalink"],
            "title": product["name"],
            "instock": product.get("is_in_stock", False),
            "price": price * self.convert_rate,
            "image": images[0]["src"] if images else None,
        }
        return book_info

    def _fetch_store_product(self, product_id: int) -> dict | None:
        response = requests.get(
            f"{self.store_api}/products/{product_id}",
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()
        logger.warning(
            f"Failed to fetch product {product_id}. Status code: {response.status_code}"
        )
        return None

    def _fetch_all_products(self) -> list[dict]:
        products: list[dict] = []
        page = 1

        while True:
            params: dict = {"per_page": self.PER_PAGE, "page": page}
            if self.category_ids:
                params["category"] = self.category_ids[0]

            response = requests.get(
                f"{self.store_api}/products",
                params=params,
                timeout=60,
            )
            if response.status_code != 200:
                raise ScraperError(
                    f"{self.name} scraper - Store API failed on page {page}. "
                    f"Status code: {response.status_code}"
                )

            batch = response.json()
            if not batch:
                logger.info("No more products found")
                break

            logger.info(f"Found {len(batch)} products on page {page}")
            products.extend(batch)

            total_pages = int(response.headers.get("X-WP-TotalPages", page))
            if page >= total_pages:
                break

            page += 1
            if self.page_delay:
                time.sleep(self.page_delay)

        return products

    def _fetch_modified_products(self, since: datetime) -> list[dict]:
        product_ids: list[int] = []
        page = 1

        if since.tzinfo is not None:
            since = since.replace(tzinfo=None)

        while True:
            params: dict = {
                "per_page": self.PER_PAGE,
                "page": page,
                "modified_after": since.isoformat(),
                "status": "publish",
            }
            response = requests.get(
                f"{self.base_url}/wp-json/wp/v2/product",
                params=params,
                timeout=60,
            )
            if response.status_code != 200:
                logger.warning(
                    f"{self.name} scraper - WP REST failed on page {page} "
                    f"(status {response.status_code}); falling back to full Store API crawl"
                )
                return self._fetch_all_products()

            batch = response.json()
            if not batch:
                break

            logger.info(f"Found {len(batch)} modified products on WP REST page {page}")
            product_ids.extend(item["id"] for item in batch)

            total_pages = int(response.headers.get("X-WP-TotalPages", page))
            if page >= total_pages:
                break

            page += 1
            if self.page_delay:
                time.sleep(self.page_delay)

        if not product_ids:
            return []

        products: list[dict] = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self._fetch_store_product, product_id): product_id
                for product_id in product_ids
            }
            for future in as_completed(futures):
                store_product = future.result()
                if store_product and self.is_target_product(store_product):
                    products.append(store_product)

        return products

    async def crawl_product_pages(self, last_crawl_success=None):
        self.test_base_url()
        if self.category_ids:
            self._get_allowed_category_ids()

        if last_crawl_success:
            logger.info(f"Fetching products modified since {last_crawl_success}")
            products = self._fetch_modified_products(last_crawl_success)
        else:
            products = self._fetch_all_products()

        books = []
        for product in products:
            if not self.is_target_product(product):
                continue
            book_info = self.extract_book_info(product)
            if book_info is not None:
                books.append(book_info)
        return books
