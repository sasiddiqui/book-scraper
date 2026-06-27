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

SITEMAP_URL = "https://jarirbooksusa.com/sitemap.xml"
PRODUCT_URL_RE = re.compile(r"/\d+\.html$")
PAGE_ATTRS_RE = re.compile(r"PAGE_ATTRS\s*=\s*(\{.*?\});", re.DOTALL)
DETAIL_ROW_RE = re.compile(r"<th>([^<:]+):\s*</th>\s*<td>([^<]+)</td>", re.I)
DESCRIPTION_RE = re.compile(
    r'class="f2 comment" itemprop="description"[^>]*>(.*?)</div>',
    re.DOTALL | re.I,
)
TITLE_RE = re.compile(
    r'id="item-contenttitle"[^>]*itemprop="name"[^>]*>([^<]+)',
    re.I,
)
IMAGE_RE = re.compile(
    r'id="photo"[^>]*src="(https://s\.turbifycdn\.com/aah/yhst-141393581866279/[^"]+)"',
    re.I,
)
HAS_PAGES_RE = re.compile(r"<th>Pages:\s*</th>\s*<td>", re.I)

NON_BOOK_SUBSTRINGS = (
    "prayer rug",
    "azan clock",
    "tisbah",
    "flags:",
    "posters",
    "prints",
    "wall art",
    "world and regional map",
    "flash cards-puzzles-games-toys",
    "kids charts - poster",
    "kids charts - pp",
)

MEDIA_ONLY_ROOTS = {"DVDs | CDs | eBooks"}
MEDIA_ONLY_SUBSTRINGS = ("quran recitations", "audio & visual for kids")
PRODUCT_SECTION_END = 'id="ys_relatedItems"'


class JarirBooks(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://jarirbooksusa.com/",
            "Jarir Books USA",
            convert_rate=1,
        )
        self.batch_size = 10
        self.batch_delay = 0.1

    def is_product_url(self, url: str) -> bool:
        return bool(PRODUCT_URL_RE.search(url))

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

        html_title = self._parse_html_title(html)
        if html_title and (not name or " > " in name):
            name = html_title

        return {
            "title": name,
            "price": price,
            "instock": self._page_attr(block, "isOrderable") == "1",
            "cat_name_path": self._page_attr(block, "catNamePath"),
        }

    def _product_section(self, html: str) -> str:
        start = html.find('id="itemarea"')
        if start < 0:
            return html

        end = html.find(PRODUCT_SECTION_END, start)
        if end < 0:
            return html[start:]
        return html[start:end]

    def _parse_html_title(self, html: str) -> str | None:
        match = TITLE_RE.search(html)
        if match:
            return match.group(1).strip()
        return None

    def _parse_product_details(self, product_html: str) -> dict[str, str | None]:
        details: dict[str, str | None] = {}
        for match in DETAIL_ROW_RE.finditer(product_html):
            key = match.group(1).strip().lower()
            value = match.group(2).strip()
            if key == "by":
                details["author"] = value
            elif key == "publisher":
                details["publisher"] = value
        return details

    def _has_pages(self, product_html: str) -> bool:
        return bool(HAS_PAGES_RE.search(product_html))

    def is_book_product(
        self, cat_name_path: str | None, product_html: str, title: str
    ) -> bool:
        cat_lower = (cat_name_path or "").lower()
        title_lower = title.lower()

        if any(substr in cat_lower for substr in NON_BOOK_SUBSTRINGS):
            return False

        root = (cat_name_path or "").split(" > ", 1)[0].strip()
        if root.startswith(">"):
            return False

        has_pages = self._has_pages(product_html)
        if root in MEDIA_ONLY_ROOTS:
            return has_pages or "book" in title_lower

        if any(substr in cat_lower for substr in MEDIA_ONLY_SUBSTRINGS):
            return has_pages or "book" in title_lower

        return True

    def _parse_description(self, product_html: str) -> str | None:
        match = DESCRIPTION_RE.search(product_html)
        if not match:
            return None
        text = BeautifulSoup(match.group(1), "lxml").get_text(" ", strip=True)
        return text or None

    def _parse_image(self, product_html: str) -> str | None:
        match = IMAGE_RE.search(product_html)
        return match.group(1) if match else None

    def _extract_from_html(self, html: str, url: str) -> dict | None:
        page_attrs = self._parse_page_attrs(html)
        if page_attrs is None:
            return None

        product_html = self._product_section(html)

        if not self.is_book_product(
            page_attrs.get("cat_name_path"), product_html, page_attrs["title"]
        ):
            return None

        book_info = {
            "url": url,
            "source": self.name,
            "title": page_attrs["title"],
            "price": page_attrs["price"],
            "instock": page_attrs["instock"],
        }

        details = self._parse_product_details(product_html)
        if details.get("author"):
            book_info["author"] = details["author"]
        if details.get("publisher"):
            book_info["publisher"] = details["publisher"]

        description = self._parse_description(product_html)
        if description:
            book_info["description"] = description

        image = self._parse_image(product_html)
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

        self.logger.info("%s: %d product URLs from sitemap", self.name, len(urls))
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

                    html = response.decode("utf-8", errors="replace")
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
