import asyncio
import time
from datetime import datetime

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper


class AlHidayaah(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.al-hidaayah.co.uk/", "Al-Hidayaah")
        self.headers["Accept-Language"] = "en-US,en;q=0.5"
        self.batch_size = 8

    def ignore_url(self, url) -> bool:
        ig = ["#"]

        return any(i in url for i in ig)

    def is_product_url(self, url):
        return url.startswith(self.base_url) and "/products/" in url

    def _product_sitemap_urls(self) -> list[str]:
        r = requests.get(
            self.base_url + "sitemap.xml",
            headers=self.headers,
            timeout=30,
        )
        r.raise_for_status()
        root = BeautifulSoup(r.text, "xml")
        out: list[str] = []
        for loc in root.find_all("loc"):
            href = (loc.text or "").strip()
            if not href:
                continue
            if "sitemap_products" not in href:
                continue
            out.append(href)
        return sorted(set(out))

    def _collect_product_urls(
        self, last_crawl_success: datetime | None
    ) -> list[str]:
        sitemap_hrefs = self._product_sitemap_urls()
        if not sitemap_hrefs:
            self.logger.warning("Al-Hidayaah: no product sitemaps in index")
            return []

        urls: list[str] = []
        for sm_url in sitemap_hrefs:
            r = requests.get(sm_url, headers=self.headers, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "xml")
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

        seen: set[str] = set()
        unique: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                unique.append(u)

        self.logger.info(
            "Al-Hidayaah: %d product URLs from %d sitemap(s)",
            len(unique),
            len(sitemap_hrefs),
        )
        return unique

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info["url"] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find(
                "h1", class_="product-meta__title heading h1"
            ).text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            price = soup.find("meta", attrs={"property": "product:price:amount"})[
                "content"
            ]
            book_info["price"] = float(price)

        except Exception:
            self.logger.warning(f"Could not find price details on {url}")

        try:
            book_info["image"] = soup.find("meta", attrs={"name": "twitter:image"})[
                "content"
            ]
        except AttributeError:
            self.logger.warning(f"Could not find image details on {url}")

        try:
            book_info["instock"] = (
                soup.find(
                    "button", class_="product-form__add-button button button--disabled"
                )
                is None
            )
        except AttributeError:
            self.logger.warning(f"Could not find stock details on {url}")

        try:
            book_info["description"] = (
                soup.find("meta", attrs={"name": "twitter:description"})["content"]
                .strip()
                .replace("\n", "")
            )
        except AttributeError:
            self.logger.warning(f"Could not find description on {url}")

        return book_info

    async def crawl_product_pages(
        self,
        last_crawl_success=None,
        initial_urls=list(),
        use_cached_links=None,
    ) -> list[dict]:
        if use_cached_links:
            return await super().crawl_product_pages(
                last_crawl_success=last_crawl_success,
                initial_urls=initial_urls,
                use_cached_links=use_cached_links,
            )

        self.logger.info(f"Crawling {self.name}")
        start = time.time()
        self.test_base_url()

        product_urls = self._collect_product_urls(last_crawl_success)

        async with aiohttp.ClientSession() as session:
            pending = list(product_urls)
            while pending:
                batch = pending[: self.batch_size]
                tasks = [
                    asyncio.create_task(self.fetch_page(session, u)) for u in batch
                ]
                for u in batch:
                    pending.remove(u)
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
                        book_info = Book(**book_info)
                        book_info.price *= self.convert_rate
                    except ValidationError as e:
                        self.logger.warning(
                            f"Could not validate book info on {url}: {e}"
                        )
                        continue

                    self.add_book(book_info)
                    self.logger.info(
                        f"SUCCESS - Added {book_info.title} to all books - {url}"
                    )

                if pending and self.batch_delay > 0:
                    await asyncio.sleep(self.batch_delay)

        self.logger.info(
            f"Finished crawling {self.name} in {time.time() - start} seconds"
        )
        return self.all_books
