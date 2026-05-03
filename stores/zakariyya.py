import asyncio
import time
from datetime import datetime

import aiohttp
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from book import Book
from scraper import AbstractBookScraper

SITEMAP_INDEX = "https://www.zakariyyabooks.com/sitemap_index.xml"


class ZakariyyaBooksScraper(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://www.zakariyyabooks.com", "Zakariyya Books", convert_rate=1.32
        )
        self.batch_size = 4

    def _product_sitemap_urls(self) -> list[str]:
        r = requests.get(SITEMAP_INDEX, headers=self.headers, timeout=30)
        r.raise_for_status()
        root = BeautifulSoup(r.text, "xml")
        out: list[str] = []
        for loc in root.find_all("loc"):
            href = (loc.text or "").strip()
            if not href:
                continue
            if "product-sitemap" not in href:
                continue
            out.append(href)
        return sorted(set(out))

    def _collect_product_urls(self, last_crawl_success: datetime | None) -> list[str]:
        sitemap_hrefs = self._product_sitemap_urls()
        if not sitemap_hrefs:
            self.logger.warning("Zakariyya Books: no product sitemaps found in index")
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
            "Zakariyya Books: %d product URLs from %d sitemap(s)%s",
            len(unique),
            len(sitemap_hrefs),
            f" (since {last_crawl_success})" if last_crawl_success else "",
        )
        return unique

    def extract_book_info(self, soup: BeautifulSoup, url):
        book_info = {}

        book_info["url"] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("h1", class_="entry-title").text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            book_info["author"] = (
                soup.find(
                    "tr",
                    class_="woocommerce-product-attributes-item--attribute_pa_book-author",
                )
                .find("td")
                .text.strip()
            )
            book_info["publisher"] = (
                soup.find(
                    "tr",
                    class_="woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_publisher",
                )
                .find("td")
                .text.strip()
            )

            price_container = soup.select("p.price.product-page-price").pop()
            # account for sale price and regular price
            sale_price = price_container.find("ins")
            if sale_price:
                book_info["price"] = sale_price.text.strip()
            else:
                book_info["price"] = price_container.find(
                    "span", class_="woocommerce-Price-amount amount"
                ).text.strip()
            if book_info["price"]:
                book_info["price"] = float(book_info["price"].replace("£", "").replace(",", ""))

        except AttributeError:
            self.logger.warning(
                f"Could not find author/publisher/price details on {url}"
            )
            return book_info

        try:
            if (img := soup.find("img", class_="wp-post-image")) and img.has_attr(
                "src"
            ):
                book_info["image"] = img["src"]

            book_info["instock"] = soup.find("p", class_="stock out-of-stock") is None

        except AttributeError:
            self.logger.warning(f"Could not find extra book details on {url}")
            return book_info

        return book_info

    def is_product_url(self, url):
        return url.startswith(self.base_url) and "/product/" in url

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


# Test parsing
if __name__ == "__main__":
    scraper = ZakariyyaBooksScraper()
    url = "https://www.zakariyyabooks.com/product/%d8%a7%d9%84%d9%85%d8%af%d9%88%d9%86%d8%a9-%d8%a7%d9%84%d8%ac%d8%a7%d9%85%d8%b9%d8%a9-%d9%84%d9%84%d8%a3%d8%ad%d8%a7%d8%af%d9%8a%d8%ab-%d8%a7%d9%84%d9%85%d8%b1%d9%88%d9%8a%d8%a9-%d8%b9%d9%86-%d8%a7/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
    else:
        import ipdb
        ipdb.set_trace()

    print(scraper.extract_book_info(soup, url))
