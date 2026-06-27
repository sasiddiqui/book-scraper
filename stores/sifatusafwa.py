import asyncio
import html
import json
import time
from datetime import datetime

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper, ScraperError

SITEMAP_URL = "https://www.sifatusafwa.com/1_en_0_sitemap.xml"
# PrestaShop product pages: /en/{category}/{slug}.html
# Sitemap also lists manufacturers, suppliers, CMS pages, and category indexes.
NON_PRODUCT_PREFIXES = ("manufacturer", "supplier", "content")


class SifatuSafwa(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://www.sifatusafwa.com", "Sifatu Safwa", convert_rate=1.2
        )
        self.batch_size = 5
        self.batch_delay = 0.2

    # ------------------------------------------------------------------
    # Cloudflare guard: a 200 that is still a challenge page must fail fast
    # ------------------------------------------------------------------

    def test_base_url(self):
        response = requests.get(
            self.base_url + "/en/", headers=self.headers, timeout=15
        )
        if response.status_code != 200 or "just a moment" in response.text.lower():
            raise ScraperError(
                f"{self.name} scraper - Cloudflare blocked access to {self.base_url}. "
                f"Status: {response.status_code}. Skipping scraper..."
            )
        return True

    # ------------------------------------------------------------------
    # Sitemap-based URL collection
    # ------------------------------------------------------------------

    def _collect_product_urls(self, last_crawl_success: datetime | None) -> list[str]:
        r = requests.get(SITEMAP_URL, headers=self.headers, timeout=60)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "xml")
        urls: list[str] = []

        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc")
            if not loc or not loc.text:
                continue
            href = loc.text.strip()
            if not self.is_product_url(href):
                continue

            if last_crawl_success:
                lm = url_tag.find("lastmod")
                if lm and lm.text:
                    try:
                        raw_lm = lm.text.strip().replace("Z", "+00:00")
                        lastmod = datetime.fromisoformat(raw_lm)
                        if lastmod.tzinfo:
                            lastmod = lastmod.replace(tzinfo=None)
                        lcs = last_crawl_success
                        if getattr(lcs, "tzinfo", None):
                            lcs = lcs.replace(tzinfo=None)
                        if lastmod < lcs:
                            continue
                    except ValueError:
                        pass

            urls.append(href)

        self.logger.info("Sifatu Safwa: %d product URLs from sitemap", len(urls))
        return urls

    # ------------------------------------------------------------------
    # URL helpers (unchanged from original)
    # ------------------------------------------------------------------

    def ignore_url(self, url) -> bool:
        ig = [
            "#",
            "SubmitCurrency=",
            "id_currency=",
            ".jpg",
            "/ar/",
            "/fr/",
            "order=",
            "/manufacturer/",
            "/supplier/",
            "/content/",
        ]
        return any(i in url for i in ig)

    def is_product_url(self, url):
        if not url.endswith(".html") or "/en/" not in url:
            return False

        prefix = f"{self.base_url.rstrip('/')}/en/"
        if not url.startswith(prefix):
            return False

        path = url[len(prefix) :]
        parts = path.split("/")
        if len(parts) != 2:
            return False

        return parts[0] not in NON_PRODUCT_PREFIXES

    # ------------------------------------------------------------------
    # Extraction: parse data-product JSON first, fall back to og meta
    # ------------------------------------------------------------------

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {"url": url, "source": self.name}

        # --- data-product JSON (most reliable source) ---
        product_data = None
        dp_tag = soup.find(attrs={"data-product": True})
        if dp_tag:
            try:
                raw = html.unescape(dp_tag["data-product"])
                product_data = json.loads(raw)
            except (json.JSONDecodeError, KeyError):
                product_data = None

        # Title
        if product_data:
            title = product_data.get("name")
        else:
            title = None

        if not title:
            og = soup.find("meta", property="og:title")
            title = og["content"] if og else None

        if not title:
            self.logger.error(f"Could not find title for {url}")
            return None
        book_info["title"] = title

        # Price
        if product_data:
            price_amount = product_data.get("price_amount")
            if price_amount is not None:
                try:
                    book_info["price"] = float(price_amount)
                except (TypeError, ValueError):
                    pass

        if "price" not in book_info:
            og_price = soup.find("meta", property="product:price:amount")
            if og_price:
                try:
                    book_info["price"] = float(og_price["content"])
                except (TypeError, ValueError):
                    pass

        # Author — from features list, fallback to manufacturer_name
        if product_data:
            author = None
            for feature in product_data.get("features", []):
                if feature.get("name") == "Author":
                    author = feature.get("value") or None
                    break
            if not author:
                author = product_data.get("manufacturer_name") or None
            if author:
                book_info["author"] = author

        # Stock — use availability field or quantity
        if product_data:
            availability = product_data.get("availability")
            if availability is not None:
                book_info["instock"] = availability == "available"
            else:
                qty = product_data.get("quantity")
                if qty is not None:
                    book_info["instock"] = int(qty) > 0

        if "instock" not in book_info:
            book_info["instock"] = soup.find("div", class_="product-unavailable") is None

        # Description — prefer description_short over truncated og:description
        if product_data:
            desc = product_data.get("description_short") or product_data.get("description")
            if desc:
                # Strip HTML tags from PrestaShop short description
                desc_soup = BeautifulSoup(desc, "lxml")
                desc_text = desc_soup.get_text(" ", strip=True)
                if desc_text:
                    book_info["description"] = desc_text

        if "description" not in book_info:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                book_info["description"] = og_desc["content"]

        # Image
        og_image = soup.find("meta", property="og:image")
        if og_image:
            book_info["image"] = og_image["content"]

        return book_info

    # ------------------------------------------------------------------
    # Crawl: sitemap-seeded, no link-following
    # ------------------------------------------------------------------

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.logger.info(f"Crawling {self.name}")
        start = time.time()

        self.test_base_url()

        product_urls = self._collect_product_urls(last_crawl_success)
        if not product_urls:
            self.logger.warning(f"{self.name}: no product URLs found in sitemap")
            return []

        async with aiohttp.ClientSession() as session:
            pending = list(product_urls)
            while pending:
                batch = pending[: self.batch_size]
                del pending[: self.batch_size]

                tasks = [
                    asyncio.create_task(self.fetch_page(session, u)) for u in batch
                ]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                for url, result in zip(batch, responses):
                    if isinstance(result, Exception):
                        self.logger.error(f"Exception while fetching {url}: {result}")
                        continue
                    _, response = result
                    if not response:
                        continue

                    soup = BeautifulSoup(response, "lxml", parse_only=self.strainer)
                    try:
                        book_info = self.extract_book_info(soup, url)
                        if book_info is None:
                            continue
                        book = Book(**book_info)
                        book.price *= self.convert_rate
                    except ValidationError as e:
                        self.logger.warning(
                            f"Could not validate book info on {url}: {e}"
                        )
                        continue

                    self.add_book(book)
                    self.logger.info(
                        f"SUCCESS - Added {book.title} to all books - {url}"
                    )

                if pending and self.batch_delay > 0:
                    await asyncio.sleep(self.batch_delay)

        self.logger.info(
            f"Finished crawling {self.name} in {time.time() - start:.1f}s — "
            f"{len(self.all_books)} books collected"
        )
        return self.all_books
