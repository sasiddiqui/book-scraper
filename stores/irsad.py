import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from scraper import AbstractBookScraper
import logging
from book import Book
import json

logger = logging.getLogger("scraper")

# Approximate TRY → USD (same order of magnitude as other Turkish store scrapers).
TRY_TO_USD = 0.027


class Irsad(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.irsad.com.tr/", "Irsad", convert_rate=1)
        self.batch_size = 10
        self.headers["Accept-Language"] = "en-US,en;q=0.9"

    def ignore_url(self, url) -> bool:
        return False

    def is_product_url(self, url):
        return not url.endswith(".jpg")

    def extract_book_info(self, html, url):
        book_info = {}
        book_info["url"] = url
        book_info["source"] = self.name

        soup = BeautifulSoup(html, "lxml")

        # Find the JSON-LD Product schema — most reliable source of product data
        product_data = None
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if data.get("@type") == "Product":
                    product_data = data
                    break
            except (json.JSONDecodeError, AttributeError):
                continue

        if product_data is None:
            self.logger.warning(
                f"Could not find JSON-LD product data for {url}. Skipping..."
            )
            return None

        book_info["title"] = product_data.get("name")

        # Price from offers (site serves TRY unless JSON-LD says otherwise)
        offers = product_data.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        raw_price = offers.get("price")
        currency = (offers.get("priceCurrency") or "").strip().upper()
        if raw_price is None:
            book_info["price"] = None
        else:
            price = float(raw_price)
            if currency == "USD":
                book_info["price"] = round(price, 2)
            else:
                if currency and currency not in {"TRY", "TL"}:
                    self.logger.info(
                        f"Unexpected priceCurrency {currency!r} on {url}; treating as TRY"
                    )
                book_info["price"] = round(price * TRY_TO_USD, 2)

        # Image — JSON-LD gives a list
        images = product_data.get("image", [])
        if isinstance(images, list) and images:
            book_info["image"] = images[0]
        elif isinstance(images, str):
            book_info["image"] = images

        # Stock — skip out-of-stock items
        availability = offers.get("availability", "")
        book_info["instock"] = "InStock" in availability
        if not book_info["instock"]:
            self.logger.info(f"Skipping {url} - out of stock")
            return None

        # Publisher (brand)
        brand = product_data.get("brand", {})
        book_info["publisher"] = (
            brand.get("name") if isinstance(brand, dict) else brand
        )

        # Author — now in a <table> row: <td>Yazar Adı</td> ... <td>author</td>
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if cells and "Yazar Ad" in cells[0].get_text():
                book_info["author"] = cells[-1].get_text(strip=True)
                break

        if "author" not in book_info:
            self.logger.info(f"Could not find author for {url}")

        return book_info

    def get_all_product_urls(self) -> list[str]:
        """Fetch sitemap index then collect all product page URLs."""

        base_sitemap = BeautifulSoup(
            requests.get(f"{self.base_url}sitemap.xml", headers=self.headers).text,
            "xml",
        )
        sitemap_urls = [
            url.text
            for url in base_sitemap.find_all("loc")
            if "product" in url.text
        ]
        logger.info(f"Found {len(sitemap_urls)} product sitemap(s). Fetching URLs...")
        product_urls = set()

        for sitemap_url in sitemap_urls:
            sitemap = BeautifulSoup(
                requests.get(sitemap_url, headers=self.headers).text, "xml"
            )
            product_urls.update(
                url.text
                for url in sitemap.find_all("loc")
                if self.is_product_url(url.text)
            )

        logger.info(f"Found {len(product_urls)} product URLs")
        return list(product_urls)

    async def crawl_product_pages(self, last_crawl_success=None):
        self.test_base_url()
        product_urls = self.get_all_product_urls()
        total_urls = len(product_urls)

        async with aiohttp.ClientSession() as session:
            while product_urls:
                batch_urls = product_urls[: self.batch_size]
                product_urls = product_urls[self.batch_size :]

                tasks = [
                    asyncio.create_task(self.fetch_page(session, url))
                    for url in batch_urls
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for result in responses:
                    if isinstance(result, Exception):
                        self.logger.error(f"Exception fetching page: {result}")
                        continue
                    url, content = result
                    if content:
                        html = content.decode("utf-8", errors="replace")
                        book_info = self.extract_book_info(html, url)
                        if book_info is not None:
                            book_info = Book(**book_info)
                            self.add_book(book_info)

                remaining = len(product_urls)
                if remaining % 100 == 0:
                    logger.info(f"Processed {total_urls - remaining}/{total_urls} URLs")

        return self.all_books
