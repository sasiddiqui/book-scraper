import asyncio
import re
import time
from datetime import datetime

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper

SITEMAP_URL = "https://www.islamicbookstore.com/sitemap.xml"
PRODUCT_URL_RE = re.compile(r"/[ab]?\d+\.html$")
PAGE_ATTRS_RE = re.compile(r"PAGE_ATTRS\s*=\s*(\{.*?\});", re.DOTALL)

BOOK_ROOTS = {
    "Books",
    "Quran",
    "School, Curriculums, Kids, and More",
    "Browse by Publisher",
    "Clearance",
}

NON_BOOK_ROOTS = {
    "Lifestyle",
    "Audio - Quran, Songs, Lectures",
}

NON_BOOK_KEYWORDS = (
    "prayer rug",
    "fragrance oil",
    "perfume oil",
    "kufi cap",
    "kufiya",
    "keffiyeh",
    "shemagh",
    "ihram set",
    "coaster set",
    "tasbeeh counter",
    "gift certificate",
)


class IslamicBookstore(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://www.islamicbookstore.com/",
            "Islamic Bookstore",
            convert_rate=1,
        )
        self.batch_size = 10
        self.batch_delay = 0.1

    def is_product_url(self, url: str) -> bool:
        return bool(PRODUCT_URL_RE.search(url))

    def is_book_product(self, cat_name_path: str | None, html: str) -> bool:
        if cat_name_path:
            root = cat_name_path.split(" > ", 1)[0]
            if root in NON_BOOK_ROOTS:
                return False
            if any(keyword in cat_name_path.lower() for keyword in NON_BOOK_KEYWORDS):
                return False
            if root in BOOK_ROOTS:
                return True

        return bool(self._parse_caption(html).get("author"))

    def _page_attr(self, page_attrs_block: str, key: str) -> str | None:
        match = re.search(rf"'{re.escape(key)}':\s*'([^']*)'", page_attrs_block)
        return match.group(1) if match else None

    def _parse_page_attrs(self, html: str) -> dict | None:
        match = PAGE_ATTRS_RE.search(html)
        if not match:
            return None

        block = match.group(1)
        sale_price = self._page_attr(block, "salePrice")
        if not sale_price:
            return None

        try:
            price = float(sale_price)
        except ValueError:
            return None

        name = self._page_attr(block, "name")
        if not name:
            return None

        is_orderable = self._page_attr(block, "isOrderable") == "1"
        in_stock_schema = "schema.org/InStock" in html
        out_of_stock_schema = "schema.org/OutOfStock" in html

        if out_of_stock_schema:
            instock = False
        elif in_stock_schema:
            instock = True
        else:
            instock = is_orderable

        return {
            "title": name,
            "price": price,
            "instock": instock,
            "cat_name_path": self._page_attr(block, "catNamePath"),
        }

    def _parse_caption(self, html: str) -> dict[str, str | None]:
        match = re.search(r'class="caption[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL | re.I)
        if not match:
            return {}

        caption_html = match.group(1)
        text = BeautifulSoup(caption_html, "lxml").get_text("\n", strip=True)

        author = None
        publisher = None
        description = None

        author_match = re.search(
            r"Author:\s*(.+?)(?=\n(?:Publisher|Pages|Binding|ISBN|Description|\Z))",
            text,
            re.I | re.S,
        )
        if author_match:
            author = author_match.group(1).strip()

        publisher_match = re.search(
            r"Publisher:\s*(.+?)(?=\n(?:Pages|Binding|ISBN|Description|\Z))",
            text,
            re.I | re.S,
        )
        if publisher_match:
            publisher = publisher_match.group(1).strip()

        description_match = re.search(
            r"Description from the publisher:\s*(.+)",
            text,
            re.I | re.S,
        )
        if description_match:
            description = " ".join(description_match.group(1).split())

        return {
            "author": author,
            "publisher": publisher,
            "description": description,
        }

    def _parse_image(self, html: str) -> str | None:
        match = re.search(
            r'src="(https://s\.turbifycdn\.com/aah/islamicbookstore-com/[^"]+)"',
            html,
        )
        return match.group(1) if match else None

    def _extract_from_html(self, html: str, url: str) -> dict | None:
        page_attrs = self._parse_page_attrs(html)
        if page_attrs is None:
            return None

        caption = self._parse_caption(html)
        if not self.is_book_product(page_attrs.get("cat_name_path"), html):
            return None

        book_info = {
            "url": url,
            "source": self.name,
            "title": page_attrs["title"],
            "price": page_attrs["price"],
            "instock": page_attrs["instock"],
        }
        if caption.get("author"):
            book_info["author"] = caption["author"]
        if caption.get("publisher"):
            book_info["publisher"] = caption["publisher"]
        if caption.get("description"):
            book_info["description"] = caption["description"]

        image = self._parse_image(html)
        if image:
            book_info["image"] = image

        return book_info

    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        return self._extract_from_html(str(soup), url)

    def _collect_product_urls(self, last_crawl_success: datetime | None) -> list[str]:
        response = requests.get(SITEMAP_URL, headers=self.headers, timeout=60)
        response.raise_for_status()

        sitemap = BeautifulSoup(response.text, "xml")
        urls: list[str] = []

        for url_tag in sitemap.find_all("url"):
            loc = url_tag.find("loc")
            if not loc or not loc.text:
                continue

            href = loc.text.strip()
            if not self.is_product_url(href):
                continue

            if last_crawl_success:
                lastmod_tag = url_tag.find("lastmod")
                if lastmod_tag and lastmod_tag.text:
                    try:
                        raw_lastmod = lastmod_tag.text.strip().replace("Z", "+00:00")
                        lastmod = datetime.fromisoformat(raw_lastmod)
                        if lastmod.tzinfo:
                            lastmod = lastmod.replace(tzinfo=None)
                        last_success = last_crawl_success
                        if getattr(last_success, "tzinfo", None):
                            last_success = last_success.replace(tzinfo=None)
                        if lastmod < last_success:
                            continue
                    except ValueError:
                        pass

            urls.append(href)

        self.logger.info(
            "%s: %d product URLs from sitemap", self.name, len(urls)
        )
        return urls

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info("Crawling %s", self.name)
        start = time.time()

        self.test_base_url()

        product_urls = self._collect_product_urls(last_crawl_success)
        if not product_urls:
            self.logger.warning("%s: no product URLs found in sitemap", self.name)
            return []

        async with aiohttp.ClientSession() as session:
            pending = list(product_urls)
            while pending:
                batch = pending[: self.batch_size]
                del pending[: self.batch_size]

                tasks = [
                    asyncio.create_task(self.fetch_page(session, url))
                    for url in batch
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for url, result in zip(batch, responses):
                    if isinstance(result, Exception):
                        self.logger.error(
                            "Exception while fetching %s: %s", url, result
                        )
                        continue

                    _, response = result
                    if not response:
                        continue

                    html = response.decode("latin-1", errors="replace")
                    try:
                        book_info = self._extract_from_html(html, url)
                        if book_info is None:
                            continue
                        book = Book(**book_info)
                        book.price *= self.convert_rate
                    except ValidationError as error:
                        self.logger.warning(
                            "Could not validate book info on %s: %s", url, error
                        )
                        continue

                    self.add_book(book)
                    self.logger.info(
                        "SUCCESS - Added %s to all books - %s", book.title, url
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
