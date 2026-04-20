# Osman Books (osmanbooks.com) — WooCommerce + Rank Math sitemaps.
# Collects all URLs from every product-sitemap*.xml listed in sitemap_index.xml,
# then parses product pages (meta tags + WooCommerce markup + optional attributes).

import asyncio
import aiohttp
import html
import logging
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from book import Book
from scraper import AbstractBookScraper

logger = logging.getLogger("scraper")

_MONEY_FLOAT_RE = re.compile(r"(\d+(?:[.,]\d+)?)")
_ARABIC_RE = re.compile(
    r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]"
)


def _parse_gbp_to_float(raw: str) -> float | None:
    if not raw:
        return None
    text = html.unescape(raw)
    text = text.replace("\xa3", "").replace("£", "").strip()
    m = _MONEY_FLOAT_RE.search(text.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


class OsmanScraper(AbstractBookScraper):
    def __init__(self):
        super().__init__(
            "https://osmanbooks.com/",
            "Osman Books",
            convert_rate=1.35,
        )
        self.batch_size = 10
        self.batch_delay = 0.1
        self.headers["Accept-Encoding"] = "gzip, deflate"

    def is_product_url(self, url: str) -> bool:
        return "/product/" in url

    def _extract_price_gbp(self, soup: BeautifulSoup) -> float | None:
        # Prefer visible WooCommerce price (correct for sales); meta as fallback.
        price_el = soup.select_one("div.summary p.price, div.entry-summary p.price")
        if not price_el:
            price_el = soup.select_one("p.price")

        if price_el:
            target = price_el.find("ins") or price_el
            bdi = target.select_one("span.woocommerce-Price-amount bdi")
            if bdi:
                gbp = _parse_gbp_to_float(bdi.get_text(" ", strip=True))
                if gbp is not None:
                    return gbp
            gbp = _parse_gbp_to_float(target.get_text(" ", strip=True))
            if gbp is not None:
                return gbp

        amt_meta = soup.find("meta", property="product:price:amount")
        cur_meta = soup.find("meta", property="product:price:currency")
        if amt_meta and amt_meta.get("content"):
            cur = (cur_meta.get("content") or "").strip().upper()
            if not cur or cur == "GBP":
                try:
                    return float(amt_meta["content"].strip())
                except ValueError:
                    pass

        return None

    def _extract_instock(self, soup: BeautifulSoup) -> bool:
        stock_p = soup.select_one("p.stock")
        if stock_p:
            classes = stock_p.get("class", [])
            if "out-of-stock" in classes:
                return False
            if "in-stock" in classes or "available-on-backorder" in classes:
                return True

        prod = soup.select_one("div.product.type-product")
        if prod:
            classes = prod.get("class", [])
            if "outofstock" in classes:
                return False
            if "instock" in classes:
                return True

        tw = soup.find("meta", attrs={"name": "twitter:data2"})
        if tw and tw.get("content"):
            return "out of stock" not in tw["content"].lower()

        return True

    def _extract_description(self, soup: BeautifulSoup) -> str | None:
        short = soup.select_one(".woocommerce-product-details__short-description")
        if short:
            text = short.get_text("\n", strip=True)
            if text:
                return text

        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return html.unescape(meta["content"]).strip() or None

        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            return html.unescape(og["content"]).strip() or None

        return None

    def _extract_author_publisher(self, soup: BeautifulSoup) -> tuple[str | None, str | None]:
        author, publisher = None, None
        for tr in soup.select("table.woocommerce-product-attributes tr, table.shop_attributes tr"):
            classes = " ".join(tr.get("class", []))
            td = tr.find("td")
            if not td:
                continue
            val = td.get_text(" ", strip=True)
            if not val:
                continue
            if "attribute_pa_book-author" in classes:
                author = val
            elif "attribute_pa_publisher" in classes:
                publisher = val
        return author, publisher

    def extract_book_info(self, soup: BeautifulSoup, url: str) -> dict | None:
        title_el = soup.select_one("h1.product_title.entry-title")
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                title = html.unescape(og["content"]).strip()
                title = re.sub(r"\s*-\s*Osman Books\s*$", "", title, flags=re.I)

        if not title:
            self.logger.warning(f"Could not find title for {url}")
            return None

        gbp = self._extract_price_gbp(soup)
        if gbp is None:
            self.logger.warning(f"Could not find GBP price for {url}")
            return None

        price_usd = round(gbp * self.convert_rate, 2)

        img_meta = soup.find("meta", property="og:image")
        image = (
            html.unescape(img_meta["content"]).strip()
            if img_meta and img_meta.get("content")
            else None
        )

        instock = self._extract_instock(soup)
        description = self._extract_description(soup)
        author, publisher = self._extract_author_publisher(soup)

        book_info: dict = {
            "url": url,
            "source": self.name,
            "title": title,
            "price": price_usd,
            "instock": instock,
        }
        if image:
            book_info["image"] = image
        if description:
            book_info["description"] = description[:50000]
        if author:
            book_info["author"] = author
        if publisher:
            book_info["publisher"] = publisher

        return book_info

    def _product_sitemap_urls(self) -> list[str]:
        idx = requests.get(
            self.base_url + "sitemap_index.xml",
            headers=self.headers,
            timeout=30,
        )
        idx.raise_for_status()
        root = BeautifulSoup(idx.text, "xml")
        out: list[str] = []
        for loc in root.find_all("loc"):
            href = (loc.text or "").strip()
            if not href:
                continue
            if "product-sitemap" not in href:
                continue
            if "product_cat" in href:
                continue
            out.append(href)
        return sorted(set(out))

    def _collect_product_urls(self, last_crawl_success: datetime | None) -> list[str]:
        sitemap_hrefs = self._product_sitemap_urls()
        if not sitemap_hrefs:
            self.logger.warning("Osman Books: no product sitemaps in index")
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

        logger.info(
            "Osman Books: %d product URLs from %d sitemap(s)",
            len(unique),
            len(sitemap_hrefs),
        )
        return unique

    async def crawl_product_pages(self, last_crawl_success=None) -> list[dict]:
        self.test_base_url()

        urls = self._collect_product_urls(last_crawl_success)

        async with aiohttp.ClientSession() as session:
            all_res: list = []
            pending = list(urls)
            while pending:
                batch = pending[: self.batch_size]
                tasks = [
                    asyncio.create_task(self.fetch_page(session, u)) for u in batch
                ]
                for u in batch:
                    pending.remove(u)
                all_res.extend(await asyncio.gather(*tasks))
                if self.batch_delay:
                    await asyncio.sleep(self.batch_delay)

            for url, response in all_res:
                if not response:
                    continue
                soup = BeautifulSoup(response, "lxml")
                book_info = self.extract_book_info(soup, url)
                if book_info is None:
                    continue
                try:
                    book = Book(**book_info)
                except Exception as e:
                    self.logger.warning(f"Could not validate book on {url}: {e}")
                    continue
                self.add_book(book)

        return self.all_books
