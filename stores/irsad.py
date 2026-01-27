import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from scraper import AbstractBookScraper
import logging
import time
from book import Book
import re
import json
import chompjs

logger = logging.getLogger("scraper")


class Irsad(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.irsad.com.tr/", "Irsad")
        self.batch_size = 20
        self.headers.update(
            {
                "Cookie": "APP_LANGUAGE=tr; is_cart_attribute_sended=false; export_cart_session_id=null; export_app=1; geoip_location_code=US; AUTOMATIC_REDIRECT=1; APP_CURRENCY=USD; APP_COUNTRY=US;"
            }
        )

    def ignore_url(self, url) -> bool:
        return False

    def is_product_url(self, url):
        return "/urun/" in url

    def extract_book_info(self, soup, url):

        soup = soup.encode('utf-8').decode('unicode_escape').encode('latin1').decode('utf-8') 
        book_info = {}
        book_info["url"] = url
        book_info["source"] = self.name

        # search the text directly
        matches = re.findall(r"pageParams\s*=\s*({.*?});", soup, re.S)

        if len(matches) < 2:
            self.logger.warning(f"Could not find pageParams for {url}. Skipping...")
            return None

        raw = matches[-1]  # second / last one

        data = chompjs.parse_js_object(raw)

        product = data.get("product")
        if product is None:
            self.logger.warning(f"Could not find product for {url}. Skipping...")
            return None

        book_info["title"] = product.get("fullName")
        book_info["price"] = product.get("salePrice") or product.get("priceWithCurrency")
        img = product.get("primaryImageUrl")
        if img:
            img = img.strip("//")
            img = f"https://{img}"
        
        book_info["image"] = img
        book_info["instock"] = product.get("quantity") > 0
        # skip 
        if not book_info["instock"]:
            self.logger.info(f"Skipping {url} - out of stock")
            return None
        book_info["publisher"] = product.get("brandName")
        author = re.search(r'<div class="product-list-title">Yazar Adı<\/div>\s*<div class="product-list-content">(.*?)<\/div>', soup)

        if author:
            book_info["author"] = author.group(1).strip()
        else:
            self.logger.info(f"Could not find author for {url}")

        return book_info

    def get_all_product_urls(self) -> list[str]:
        """Find all sitemap urls then get product urls"""

        base_sitemap = BeautifulSoup(
            requests.get(f"{self.base_url}sitemap.xml").text, "xml"
        )
        sitemap_urls = [
            url.text
            for url in base_sitemap.find_all("loc")
            if "product" in url.text
        ]
        logger.info(f"Found {len(sitemap_urls)} sitemap urls. Fetching product urls...")
        product_urls = set()

        for sitemap_url in sitemap_urls:
            sitemap = BeautifulSoup(requests.get(sitemap_url).text, "xml")
            product_urls.update(
                [
                    url.text
                    for url in sitemap.find_all("loc")
                    if self.is_product_url(url.text)
                ]
            )
        logger.info(f"Found {len(product_urls)} product urls")
        return list(product_urls)

    async def crawl_product_pages(self):
        self.test_base_url()
        product_urls = self.get_all_product_urls()
        total_urls = len(product_urls)

        async with aiohttp.ClientSession() as session:
            while len(product_urls) > 0:
                batch_urls = product_urls[: self.batch_size]
                tasks = []
                for url in batch_urls:
                    tasks.append(asyncio.create_task(self.fetch_page(session, url)))
                    product_urls.remove(url)
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for url, response in responses:
                    if response:
                        book_info = self.extract_book_info(str(response), url)
                        if book_info is not None:
                            book_info = Book(**book_info)
                            self.add_book(book_info)
                if len(product_urls) % 100 == 0:
                    logger.info(f"Processed {product_urls}/{total_urls} product urls")

        return self.all_books
