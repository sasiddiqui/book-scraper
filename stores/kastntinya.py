import re

from scraper import AbstractBookScraper
from bs4 import BeautifulSoup

# Zid's og:title is the SEO title, not the product name. On Arabic pages it is
# often a Latin transliteration capped at 60 characters, while the h1 is the
# full Arabic name.
_ARABIC_RE = re.compile(r"[\u0600-\u06FF]")


def _split_name(raw: str) -> list[str]:
    parts = raw.split(" | ")
    if len(parts) != 3:
        parts = parts[0].split(" - ")
    return [part.strip() for part in parts if part.strip()]


def _visible_name(soup: BeautifulSoup) -> str | None:
    heading = soup.find("h1")
    if heading:
        text = heading.get_text(" ", strip=True)
        if text:
            return text
    return None


def _english_name(soup: BeautifulSoup) -> str | None:
    og = soup.find("meta", property="og:title")
    if not og or not og.get("content"):
        return None
    og_title = og["content"].strip()
    if not og_title or _ARABIC_RE.search(og_title):
        return None

    # The SEO title is truncated. The meta description sometimes continues that
    # same Latin string past the 60-character cap.
    description = soup.find("meta", attrs={"name": "description"})
    if description and description.get("content"):
        first_line = description["content"].split("\n", 1)[0].strip()
        if (
            first_line
            and not _ARABIC_RE.search(first_line)
            and first_line.startswith(og_title)
        ):
            return first_line
    return og_title


def _with_english_title(arabic_title: str, english_name: str | None) -> str:
    if not english_name:
        return arabic_title
    parts = _split_name(english_name)
    if not parts:
        return arabic_title
    english_title = parts[0]
    if english_title.casefold() == arabic_title.casefold():
        return arabic_title
    return f"{arabic_title}\n{english_title}"


class Kastntinya(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://kastntinya.com", "Kastntinya")
        self.batch_size = 25

    def ignore_url(self, url) -> bool:
        ig = [
            "/auth",
            "/login",
            "javascript",
            "/review",
            "/search/",
            "/cart/",
            "/checkout/",
            "/account/",
            "#",
            "?",
        ]
        return any(i in url for i in ig)

    def is_product_url(self, url):
        return "/products/" in url

    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        book_info = {}

        book_info["url"] = url
        book_info["source"] = self.name

        try:
            visible = _visible_name(soup)
            english = _english_name(soup)
            raw_name = visible or english
            if not raw_name:
                raise ValueError("missing title")

            title = _split_name(raw_name)
            book_info["title"] = title[0]
            if visible and _ARABIC_RE.search(visible):
                book_info["title"] = _with_english_title(title[0], english)
        except Exception as e:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            book_info["price"] = float(
                soup.find("h2", class_="product-formatted-price theme-text-primary")
                .text.strip()
                .replace("$", "")
            )
        except Exception as e:
            self.logger.warning(f"Could not find price for {url}")
            return None

        try:
            book_info["instock"] = (
                soup.find("meta", property="product:availability")["content"]
                == "in stock"
            )
        except Exception as e:
            self.logger.warning(f"Could not find instock for {url}")

        if len(title) == 3:
            book_info["author"] = title[1].strip()
            book_info["publisher"] = title[2].strip()
        elif len(title) == 2:
            book_info["publisher"] = title[1].strip()
            self.logger.info(f"Could not find author for {url}")
        else:
            self.logger.info(f"Could not find author/publisher for {url}")

        try:
            book_info["image"] = soup.find("meta", property="og:image")["content"]
        except Exception as e:
            self.logger.info(f"Could not find image for {url}")

        return book_info
