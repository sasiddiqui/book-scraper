# this scraper takes advantage of the sitemap at xmlsitemap.php?type=products
# the site is a BigCommerce Stencil store - most fields come from meta tags,
# but publisher and author live in the custom-fields <dl> so we pull them with regex.

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from scraper import AbstractBookScraper
import time
from book import Book
import logging
import re

logger = logging.getLogger("scraper")


PUBLISHER_RE = re.compile(
    r'<dt[^>]*class="productView-info-name"[^>]*>\s*Publisher Name:\s*</dt>\s*'
    r'<dd[^>]*class="productView-info-value"[^>]*>\s*(.*?)\s*</dd>',
    re.IGNORECASE | re.DOTALL,
)
AUTHOR_RE = re.compile(
    r'<dt[^>]*class="productView-info-name"[^>]*>\s*Author Name:\s*</dt>\s*'
    r'<dd[^>]*class="productView-info-value"[^>]*>\s*(.*?)\s*</dd>',
    re.IGNORECASE | re.DOTALL,
)


class DarulHikmah(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://islamicbookcenter.org/", "Darul Hikmah Bookstore"
        )
        self.batch_size = 10
        self.batch_delay = 0.1
        self.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:145.0) Gecko/20100101 Firefox/145.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )

    def is_product_url(self, url):
        return True

    def extract_book_info(self, response: bytes, url: str) -> dict | None:
        # BigCommerce pages are large; parse the meta block with BeautifulSoup
        # but keep the raw html around for regex-based custom-field lookup.
        try:
            html = response.decode("utf-8", errors="replace")
        except Exception:
            html = str(response)

        soup = BeautifulSoup(html, "lxml")

        book_info = {"url": url, "source": self.name}

        title_tag = soup.find("meta", property="og:title")
        if not title_tag or not title_tag.get("content"):
            self.logger.warning(f"Could not find title for {url}")
            return None
        book_info["title"] = title_tag["content"].strip()

        price_tag = soup.find("meta", property="product:price:amount") or soup.find(
            "meta", property="og:price:amount"
        )
        if not price_tag or not price_tag.get("content"):
            self.logger.warning(f"Could not find price for {url}")
            return None
        try:
            book_info["price"] = float(price_tag["content"].strip())
        except ValueError:
            self.logger.warning(f"Could not parse price for {url}")
            return None

        image_tag = soup.find("meta", property="og:image")
        if image_tag and image_tag.get("content"):
            book_info["image"] = image_tag["content"].strip()

        availability_tag = soup.find("meta", property="og:availability")
        if availability_tag and availability_tag.get("content"):
            book_info["instock"] = (
                availability_tag["content"].strip().lower() == "instock"
            )
        else:
            # fallback to schema.org markers in the rendered page
            book_info["instock"] = "http://schema.org/InStock" in html

        publisher_match = PUBLISHER_RE.search(html)
        if publisher_match:
            publisher = publisher_match.group(1).strip()
            if publisher:
                book_info["publisher"] = publisher

        author_match = AUTHOR_RE.search(html)
        if author_match:
            author = author_match.group(1).strip()
            if author:
                book_info["author"] = author

        return book_info

    def _get_product_urls(self) -> list[str]:
        """Walk the paginated product sitemap until a page has no <loc> entries."""
        product_urls: list[str] = []
        page = 1
        while True:
            sitemap_url = (
                f"{self.base_url}xmlsitemap.php?type=products&page={page}"
            )
            try:
                resp = requests.get(sitemap_url, headers=self.headers, timeout=30)
            except requests.RequestException as e:
                logger.warning(f"DarulHikmah - failed to fetch {sitemap_url}: {e}")
                break

            if resp.status_code != 200 or not resp.text.strip():
                break

            sitemap = BeautifulSoup(resp.text, "xml")
            page_urls = [loc.text for loc in sitemap.find_all("loc")]
            if not page_urls:
                break

            product_urls.extend(page_urls)
            logger.info(
                f"DarulHikmah - sitemap page {page}: {len(page_urls)} urls"
            )
            page += 1

        # de-dupe while keeping order
        seen = set()
        unique_urls = []
        for u in product_urls:
            if u not in seen:
                seen.add(u)
                unique_urls.append(u)
        return unique_urls

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.test_base_url()

        urls = self._get_product_urls()
        logger.info(f"DarulHikmah - Found {len(urls)} product URLs")

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
                if book_info is not None:
                    try:
                        book = Book(**book_info)
                    except Exception as e:
                        logger.warning(f"Could not validate book on {url}: {e}")
                        continue
                    self.add_book(book)

        return self.all_books
