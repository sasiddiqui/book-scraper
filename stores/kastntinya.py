from scraper import AbstractBookScraper
from bs4 import BeautifulSoup

class Kastntinya(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://kastntinya.com", "Kastntinya")

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

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            title = soup.find('meta', property="og:title")["content"].split(" | ")
            if len(title) != 3:
                title = title[0].split(" - ")

            book_info['title'] = title[0].strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            book_info['price'] = float(soup.find('h2', class_="product-formatted-price theme-text-primary").text.strip().replace("$", ""))
        except AttributeError:
            self.logger.warning(f"Could not find price for {url}")
            return None
        
        try:
            book_info['instock'] = soup.find('meta', property="product:availability")["content"] == "in stock"
        except AttributeError:
            self.logger.warning(f"Could not find instock for {url}")
        

        if len(title) == 3:
            book_info['author'] = title[1].strip()
            book_info['publisher'] = title[2].strip()
        elif len(title) == 2:
            book_info['publisher'] = title[1].strip()
            self.logger.info(f"Could not find author for {url}")
        else:
            self.logger.info(f"Could not find author/publisher for {url}")

        

        try:
            book_info['image'] = soup.find('meta', property="og:image")["content"]
        except AttributeError:
            self.logger.info(f"Could not find image for {url}")

        return book_info
        