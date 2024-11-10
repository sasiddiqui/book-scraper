import re
import urllib
from scraper import AbstractBookScraper

class Kunuz(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.alkunuz.co.uk", "Al-Kunuz", convert_rate=1.3)
        self.batch_size = 10
    
    def ignore_url(self, url) -> bool:
        return False
    
    def is_product_url(self, url):
        return "/product-page/" in url
    
    def extract_book_info(self, soup, url):
        book_info = {}
        book_info["source"] = self.name
        # escape the url for arabic characters
        book_info["url"] = urllib.parse.quote(url, safe=':/')

        try:
            book_info["title"] = soup.find("meta", property="og:title")["content"].replace("| al-kunuz", "").strip()
        except Exception as e:
            self.logger.error(f"Could not find title for {url}")
            return None
        
        try:
            book_info["price"] = soup.find("meta", property="product:price:amount")["content"].replace("£", "").replace(" ", "")
            book_info["price"] = float(book_info["price"]) 
        except Exception as e:
            self.logger.error(f"Could not find price for {url}")
            return None
        
        try:
            book_info["author"] = soup.find("h2", text=re.compile("Author")).find_next("p").text.strip()
        except Exception as e:
            self.logger.error(f"Could not find author for {url}")

        try:
            book_info["image"] = soup.find("meta", property="og:image")["content"]
        except Exception as e:
            self.logger.error(f"Could not find image for {url}")
        
        try:
            book_info["instock"] = soup.find("meta", property="og:availability")["content"] == "InStock"
        except Exception as e:
            self.logger.error(f"Could not find instock for {url}")

        return book_info
    
