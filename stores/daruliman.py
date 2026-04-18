# Darulimanbooks Inc (darulimanbooks.com) - WooCommerce store.
# All the data we need (title, image, price, availability) is already in the
# JSON-LD "Product" block on each product page, so we parse that.
# URLs come from the WordPress product sitemap.

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from scraper import AbstractBookScraper
import time
from datetime import datetime
from book import Book
import logging
import json
import re

logger = logging.getLogger("scraper")

PRODUCT_LD_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


class Daruliman(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://darulimanbooks.com/", "Darul Iman Books")
        self.batch_size = 5
        self.batch_delay = 0.1
        # server ships brotli by default; limit to what aiohttp handles natively
        self.headers["Accept-Encoding"] = "gzip, deflate"

    def is_product_url(self, url):
        return "/product/" in url

    def _iter_ld_objects(self, html: str):
        """Yield every JSON object found in <script type=application/ld+json> blocks."""
        for match in PRODUCT_LD_RE.finditer(html):
            raw = match.group(1).strip()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            # Can be a dict, a list, or a {"@graph": [...]} wrapper.
            if isinstance(data, list):
                yield from data
            elif isinstance(data, dict):
                if isinstance(data.get("@graph"), list):
                    yield from data["@graph"]
                else:
                    yield data

    def _find_product_ld(self, html: str) -> dict | None:
        for obj in self._iter_ld_objects(html):
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return obj
        return None

    def _extract_price_and_stock(self, product_ld: dict):
        price = None
        instock = False

        offers = product_ld.get("offers")
        if isinstance(offers, dict):
            offers = [offers]
        if not isinstance(offers, list):
            offers = []

        for offer in offers:
            if not isinstance(offer, dict):
                continue

            availability = offer.get("availability", "")
            if isinstance(availability, str) and "InStock" in availability:
                instock = True

            if price is None:
                p = offer.get("price")
                if p is None:
                    specs = offer.get("priceSpecification")
                    if isinstance(specs, dict):
                        specs = [specs]
                    if isinstance(specs, list):
                        for spec in specs:
                            if isinstance(spec, dict) and spec.get("price") is not None:
                                p = spec.get("price")
                                break
                if p is not None:
                    try:
                        price = float(p)
                    except (TypeError, ValueError):
                        pass

        return price, instock

    def extract_book_info(self, response, url: str) -> dict | None:
        if isinstance(response, (bytes, bytearray)):
            html = response.decode("utf-8", errors="replace")
        else:
            html = str(response)

        product = self._find_product_ld(html)
        if not product:
            self.logger.warning(f"No Product JSON-LD on {url}")
            return None

        title = product.get("name")
        if not title or not str(title).strip():
            self.logger.warning(f"Could not find title for {url}")
            return None

        price, instock = self._extract_price_and_stock(product)
        if price is None:
            self.logger.warning(f"Could not find price for {url}")
            return None

        image = product.get("image")
        if isinstance(image, list):
            image = image[0] if image else None
        if isinstance(image, dict):
            image = image.get("url")

        book_info = {
            "url": url,
            "source": self.name,
            "title": str(title).strip(),
            "price": price,
            "instock": instock,
        }
        if image:
            book_info["image"] = image

        return book_info

    def _get_product_urls(self, last_crawl_success=None) -> list[str]:
        sitemap_url = self.base_url + "wp-sitemap-posts-product-1.xml"
        resp = requests.get(sitemap_url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        print(resp.text)

        soup = BeautifulSoup(resp.text, "xml")
        urls: list[str] = []
        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc")
            if not loc:
                continue
            href = loc.text.strip()
            print(href)
            if not self.is_product_url(href):
                continue

            lastmod_tag = url_tag.find("lastmod")
            if last_crawl_success and lastmod_tag:
                try:
                    lastmod = datetime.fromisoformat(lastmod_tag.text)
                    if lastmod.tzinfo:
                        lastmod = lastmod.replace(tzinfo=None)
                    if lastmod < last_crawl_success:
                        continue
                except ValueError:
                    pass

            urls.append(href)

        logger.info(f"Daruliman - Found {len(urls)} product URLs in sitemap")
        return urls

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.test_base_url()

        urls = self._get_product_urls(last_crawl_success=last_crawl_success)

        async with aiohttp.ClientSession() as session:
            all_res = []
            while len(urls) > 0:
                batch_urls = urls[: self.batch_size]
                tasks = []
                for url in batch_urls:
                    tasks.append(asyncio.create_task(self.fetch_page(session, url)))
                    urls.remove(url)

                all_res.extend(await asyncio.gather(*tasks))
                time.sleep(self.batch_delay)

            for url, response in all_res:
                if not response:
                    continue
                book_info = self.extract_book_info(response, url)
                if book_info is None:
                    continue
                try:
                    book = Book(**book_info)
                except Exception as e:
                    logger.warning(f"Could not validate book on {url}: {e}")
                    continue
                self.add_book(book)

        return self.all_books
