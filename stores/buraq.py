import re
import urllib.parse
from scraper import AbstractBookScraper

class Buraq(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://buraqbooks.com")
        self.name = "buraq"
        self.batch_size = 10

        self.headers["Accept-Language"] = "en-US,en;q=0.5"
    
    def ignore_url(self, url) -> bool:
        return "#" in url
    
    def is_product_url(self, url):
        return "/products/" in url
    
    def extract_book_info(self, soup, url):


        book_info = {}
        book_info["url"] = soup.find("meta", property="og:url").get("content")
        book_info["source"] = "Buraq Books"

        try:
            book_info["title"] = soup.find("meta", property="og:title").get("content")
        except Exception as e:
            self.logger.error(f"Could not find title for {url}")
            return None

        try:
            book_info["price"] = soup.find("meta", property="og:price:amount").get("content")
        except Exception as e:
            self.logger.error(f"Could not find price for {url}")
            return None

        try:
            # search for "Author: " in a p tag
            book_info["author"] = soup.find("p", text=re.compile("Author:")).text.split(":")[1].strip()
        except Exception as e:
            self.logger.error(f"Could not find author for {url}")

        
        try:
            book_info["instock"] = soup.find("button", class_="product-form__submit button button--full-width button--secondary").text.strip() == "Add to cart"
        except Exception as e:
            self.logger.error(f"Could not find instock for {url}")
        
        try:
            book_info["image"] = soup.find("meta", property="og:image:secure_url").get("content")
        except Exception as e:
            self.logger.error(f"Could not find image for {url}")
        
        return book_info
