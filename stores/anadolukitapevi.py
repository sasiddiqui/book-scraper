# Scraper for https://www.anadolukitapevi.com/
# - All product/category URLs live in /sitemap.xml
# - Product pages have priority >= 0.95 (categories/static pages have lower)
# - Product pages expose schema.org Product microdata (price, currency, stock)
# - Author lives inside `.prd_brand_box .writers .writer`
# - Publisher lives inside `.prd_brand_box > .publisher` (top-level only;
#   related products further down also use these classes, so we must scope
#   to the brand box).

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from scraper import AbstractBookScraper
import time
from book import Book
import logging

logger = logging.getLogger("scraper")


# rough TRY -> USD conversion. Most products are priced in TRY; some sellers
# list directly in USD (the schema.org priceCurrency reflects this per-product).
TRY_TO_USD = 0.026
PRODUCT_PRIORITY_THRESHOLD = 0.95


class Anadolukitapevi(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.anadolukitapevi.com/", "Anadolu Kitabevi")
        self.batch_size = 15
        self.batch_delay = 0.1

    def is_product_url(self, url):
        return True

    def _get_product_urls(self) -> list[str]:
        resp = requests.get(
            f"{self.base_url}sitemap.xml", headers=self.headers, timeout=30
        )
        resp.raise_for_status()
        sitemap = BeautifulSoup(resp.text, "xml")

        urls = []
        seen = set()
        for url_tag in sitemap.find_all("url"):
            loc = url_tag.find("loc")
            priority = url_tag.find("priority")
            if not loc or not priority:
                continue
            try:
                if float(priority.text) < PRODUCT_PRIORITY_THRESHOLD:
                    continue
            except ValueError:
                continue
            link = loc.text.strip()
            # the homepage sneaks in at priority 1.0; skip anything that
            # clearly isn't a product slug.
            if link.rstrip("/") == self.base_url.rstrip("/"):
                continue
            if link in seen:
                continue
            seen.add(link)
            urls.append(link)
        return urls

    def extract_book_info(self, html: str, url: str) -> dict | None:
        soup = BeautifulSoup(html, "lxml")

        # The schema.org Product block is the marker that this is actually a
        # product page (some priority>=0.95 URLs are author/publisher landing
        # pages and won't have it).
        product_node = soup.find(attrs={"itemtype": "https://schema.org/Product"})
        if product_node is None:
            self.logger.info(f"Not a product page, skipping: {url}")
            return None

        book_info = {"url": url, "source": self.name}

        title_tag = soup.find("meta", property="og:title")
        if title_tag and title_tag.get("content"):
            book_info["title"] = title_tag["content"].strip()
        else:
            name_node = product_node.find(attrs={"itemprop": "name"})
            if not name_node:
                self.logger.warning(f"Could not find title for {url}")
                return None
            book_info["title"] = name_node.get_text(strip=True)

        offer_node = product_node.find(attrs={"itemprop": "offers"})
        if not offer_node:
            self.logger.warning(f"Could not find offer for {url}")
            return None

        price_node = offer_node.find(attrs={"itemprop": "price"})
        currency_node = offer_node.find(attrs={"itemprop": "priceCurrency"})
        if not price_node:
            self.logger.warning(f"Could not find price for {url}")
            return None
        try:
            price = float(
                (price_node.get("content") or price_node.get_text(strip=True)).replace(
                    ",", "."
                )
            )
        except (ValueError, AttributeError):
            self.logger.warning(f"Could not parse price for {url}")
            return None

        currency = ""
        if currency_node:
            currency = (
                currency_node.get("content") or currency_node.get_text(strip=True) or ""
            ).strip().upper()
        if currency in {"TRY", "TL"}:
            price = price * TRY_TO_USD
        elif currency and currency != "USD":
            self.logger.info(f"Unexpected currency {currency} on {url}; assuming USD")
        book_info["price"] = round(price, 2)

        availability_node = offer_node.find(attrs={"itemprop": "availability"})
        if availability_node and availability_node.get("content"):
            book_info["instock"] = (
                "InStock" in availability_node["content"]
            )
        else:
            book_info["instock"] = True

        image_tag = soup.find("meta", property="og:image")
        if image_tag and image_tag.get("content"):
            book_info["image"] = image_tag["content"].strip()

        brand_box = soup.find(class_="prd_brand_box")
        if brand_box:
            writers = brand_box.find(class_="writers")
            if writers:
                authors = [
                    a.get_text(strip=True)
                    for a in writers.find_all(class_="writer")
                ]
                authors = [a for a in authors if a]
                if authors:
                    book_info["author"] = ", ".join(authors)

            # The publisher anchor is a direct child of .prd_brand_box (not
            # nested inside .writers), so scope strictly to direct children to
            # avoid picking up related-product publishers later in the page.
            publisher_node = brand_box.find(
                "a", class_="publisher", recursive=False
            )
            if publisher_node:
                publisher = publisher_node.get_text(strip=True)
                if publisher:
                    book_info["publisher"] = publisher

        # Fall back to the schema.org brand if the brand box didn't yield one.
        if "publisher" not in book_info:
            brand_node = product_node.find(attrs={"itemprop": "brand"})
            if brand_node:
                brand_name = brand_node.find(attrs={"itemprop": "name"})
                if brand_name:
                    publisher = (
                        brand_name.get("content") or brand_name.get_text(strip=True)
                    )
                    if publisher:
                        book_info["publisher"] = publisher.strip()

        return book_info

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.test_base_url()

        urls = self._get_product_urls()
        logger.info(f"{self.name} - Found {len(urls)} candidate product URLs")

        async with aiohttp.ClientSession() as session:
            while urls:
                batch_urls = urls[: self.batch_size]
                urls = urls[self.batch_size :]

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
                    if not content:
                        continue
                    html = content.decode("utf-8", errors="replace")
                    book_info = self.extract_book_info(html, url)
                    if book_info is None:
                        continue
                    try:
                        book = Book(**book_info)
                    except Exception as e:
                        logger.warning(f"Could not validate book on {url}: {e}")
                        continue
                    self.add_book(book)

                if urls and self.batch_delay > 0:
                    time.sleep(self.batch_delay)

        return self.all_books
