# this scraper takes advantage of the sitemap

import asyncio
import aiohttp
from bs4 import BeautifulSoup, SoupStrainer
import requests
from scraper import ScraperError, AbstractBookScraper
import time
from book import Book
import logging

logger = logging.getLogger("scraper")


class JquBookstore(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://jqubookstore.com/", "JQU Bookstore", convert_rate=0.73
        )
        self.strainer = SoupStrainer("meta")
        self.batch_size = 10
        self.batch_delay = 0.1
        self.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:145.0) Gecko/20100101 Firefox/145.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Sec-GPC": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )

    def is_product_url(self, url):
        return True

    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        book_info = {}

        book_info["url"] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("meta", property="og:title")[
                "content"
            ].strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            book_info["price"] = float(
                soup.find("meta", property="og:price:amount")["content"].strip()
            )
        except AttributeError:
            self.logger.warning(f"Could not find price for {url}")
            return None

        try:
            book_info["image"] = soup.find("meta", property="og:image")[
                "content"
            ].strip()
        except Exception as e:
            self.logger.info(f"Could not find image for {url}")

        try:
            book_info["instock"] = "http://schema.org/InStock" in str(soup)
        except AttributeError:
            self.logger.info(f"Could not find instock for {url}")

        return book_info

    async def crawl_product_pages(self) -> list[dict]:

        self.test_base_url()

        base_sitemap = BeautifulSoup(
            requests.get(self.base_url + "sitemap.xml").text, "xml"
        )
        # get the updated sitemap everytime
        sitemap_url = [
            url.text
            for url in base_sitemap.find_all("loc")
            if "sitemap_products" in url.text
        ][0]
        sitemap = BeautifulSoup(requests.get(sitemap_url).text, "xml")
        logger.info("JQUBookstore - Successfully fetched sitemap")

        urls = [
            url.text
            for url in sitemap.find_all("loc")
            if "file" not in url.text and "/products/" in url.text
        ]

        logger.info(f"JQUBookstore - Found {len(urls)} product URLs")

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
                if response:
                    soup = BeautifulSoup(
                        response,
                        "lxml",
                    )

                    book_info = self.extract_book_info(soup, url)
                    if book_info is not None:
                        book_info = Book(**book_info)
                        self.add_book(book_info)

        return self.all_books
