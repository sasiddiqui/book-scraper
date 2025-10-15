import re
from scraper import AbstractBookScraper
from bs4 import SoupStrainer

class AlBalagh(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.albalaghbooks.com", "Al-Balagh")
        self.strainer = SoupStrainer(["div", "h1"], class_=["span9 ty-product-block__left", "ty-product-block-title", "ty-product-img cm-preview-wrapper"])
        self.batch_size = 20
    
    def ignore_url(self, url) -> bool:
        ig = [
            "twitter",
            "kid",
            "pen",
            "pinterest",
            "jpg",
            "png",
            "add_product",
            "quick_view",
            "color",
            "#",
            "selected_section",
            "login",
            "?",
            "-cd-"
        ]
        return any(i in url for i in ig)

    def is_product_url(self, url) -> bool:
        # no way to identify product url. Just have to parse and see if it has the book elements or not
        return True
    
    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}
        book_info["url"] = url
        book_info["source"] = self.name
        try: 
            book_info["title"] = soup.find("h1", class_="ty-product-block-title").text.strip()
        except Exception as e:
            self.logger.error(f"Could not find title for {url}")
            return None
        
        try:
            book_info["author"] = soup.find("span", class_="ty-product-feature__label", text=re.compile("Author")).find_next("em").text.strip()
        except Exception as e:
            self.logger.error(f"Could not find author for {url}")
            
        try:
            book_info["price"] = float(soup.find("div", class_="ty-product-block__price-actual").text.strip().replace("$", ""))
            if book_info["price"]:
                book_info["price"] = float(book_info["price"])

        except Exception as e:
            self.logger.error(f"Could not find price for {url}")
            return None
        
        try:
            book_info["image"] = soup.find("img")["src"]
        except Exception as e:
            self.logger.error(f"Could not find image for {url}")

        try:
            book_info["instock"] = soup.find("span", class_="ty-qty-in-stock") is not None
        except Exception as e:
            self.logger.error(f"Could not find instock for {url}")

        return book_info