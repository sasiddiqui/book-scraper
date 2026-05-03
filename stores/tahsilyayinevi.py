import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup, SoupStrainer
from scraper import AbstractBookScraper
from book import Book

# TRY → USD conversion rate (approximate)
TRY_TO_USD = 0.027


def _parse_try_price(text: str) -> float | None:
    """
    Parse a Turkish-format price string like "2.476,46 TL" → 2476.46
    Turkish number format uses '.' as thousands separator and ',' as decimal.
    """
    cleaned = text.replace("TL", "").strip()
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


class TahsilYayinevi(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.tahsilyayinevi.com/", "Tahsil Yayinevi", convert_rate=TRY_TO_USD)
        self.strainer = SoupStrainer(["h1", "div", "meta", "a", "span", "strong"])
        self.batch_size = 3
        self.batch_delay = 1

    def is_product_url(self, url):
        return "/urun/" in url

    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        book_info = {}
        book_info["url"] = url
        book_info["source"] = self.name

        # Title
        try:
            book_info["title"] = soup.find("div", class_="product-title").find("h1").get_text(strip=True)
        except Exception:
            self.logger.warning(f"Could not find title for {url}")
            return None

        # Price — prefer the sale/discounted price, fall back to the regular price
        try:
            price_el = soup.find("div", class_="product-price-new") or soup.find("div", class_="product-price-old")
            if price_el:
                raw = price_el.get_text(strip=True)
            else:
                # No discount structure — price is directly in product-price
                raw = soup.find("div", class_="product-price").get_text(strip=True)

            price = _parse_try_price(raw)
            if price is None or price < 0:
                self.logger.warning(f"Invalid price '{raw}' for {url}")
                return None
            book_info["price"] = price
        except Exception:
            self.logger.warning(f"Could not find price for {url}")
            return None

        # Image
        try:
            book_info["image"] = soup.find("meta", property="og:image")["content"]
        except Exception:
            self.logger.info(f"Could not find image for {url}")

        # In stock — "Sepete Ekle" (add to cart) link means in stock;
        # "Gelince Haber Ver" (notify me) link means out of stock
        try:
            book_info["instock"] = soup.find("a", class_="add-to-cart-button") is not None
        except Exception:
            self.logger.info(f"Could not determine stock for {url}")

        # Author — inside div.product-brand > a > strong
        try:
            brand_div = soup.find("div", class_="product-brand")
            if brand_div:
                strong = brand_div.find("strong")
                if strong:
                    book_info["author"] = strong.get_text(strip=True)
        except Exception:
            self.logger.info(f"Could not find author for {url}")

        # Publisher — in a product-list-row where the title cell is "Yayınevi"
        try:
            for row in soup.find_all("div", class_="product-list-row"):
                label = row.find("div", class_="product-list-title")
                if label and "Yayınevi" in label.get_text():
                    content = row.find("div", class_="product-list-content")
                    if content:
                        book_info["publisher"] = content.get_text(strip=True)
                    break
        except Exception:
            self.logger.info(f"Could not find publisher for {url}")

        return book_info

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.test_base_url()

        # Collect all product URLs from both product sitemaps
        urls = []
        for sitemap_path in [
            "xml/sitemap_product_1.xml",
            "xml/sitemap_product_2.xml",
        ]:
            sitemap_url = self.base_url + sitemap_path
            try:
                resp = requests.get(sitemap_url, headers=self.headers, timeout=15)
                xml_soup = BeautifulSoup(resp.text, "xml")
                urls.extend(loc.text for loc in xml_soup.find_all("loc"))
            except Exception as e:
                self.logger.error(f"Could not fetch sitemap {sitemap_url}: {e}")

        self.logger.info(f"Found {len(urls)} product URLs from sitemaps")

        async with aiohttp.ClientSession() as session:
            all_res = []
            remaining = list(urls)
            while remaining:
                batch = remaining[: self.batch_size]
                remaining = remaining[self.batch_size :]
                tasks = [
                    asyncio.create_task(self.fetch_page(session, url))
                    for url in batch
                ]
                all_res.extend(await asyncio.gather(*tasks))
                if remaining and self.batch_delay:
                    await asyncio.sleep(self.batch_delay)

        for url, response in all_res:
            if response:
                soup = BeautifulSoup(response, "lxml", parse_only=self.strainer)
                book_info = self.extract_book_info(soup, url)
                if book_info is not None:
                    try:
                        book = Book(**book_info)
                        book.price *= self.convert_rate
                        self.add_book(book)
                    except Exception as e:
                        self.logger.warning(f"Could not validate book for {url}: {e}")

        return self.all_books
