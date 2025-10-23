import json
from scraper import AbstractBookScraper
from bs4 import BeautifulSoup
import re
class Salafi(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://salafibookstore.com", "Salafi Books", convert_rate=1.33)
        self.batch_size = 15
        self.test_urls = [
            "https://salafibookstore.com/product/taleeq-ala-meemiyyah-ibn-al-qayyim/",
            "https://salafibookstore.com/product/the-book-of-manners/",
            "https://salafibookstore.com/product/muntaki-min-fataawa-fadheelah-al-shaykh-saalih-bin-fawzaan-bin-abdullah-al-fawzaan/",

        ]
    
    def ignore_url(self, url):
        ig = [
            "?add-to-cart=",
            "#",

        ]
        return any(i in url for i in ig)
    
    def extract_book_info(self, soup: BeautifulSoup, url):
        book_info = {
            "url": url,
            "source": self.name,
        }

        # we only want the books not other stuff
        breadcrumb = soup.find("nav", class_="woocommerce-breadcrumb").text.strip().lower()
        if "books" not in breadcrumb:
            self.logger.info(f"Skipping {url} because it is not a book")
            return None

        try:
            book_info["title"] = soup.find("meta", property="og:title")["content"].strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None
        
        try:
            price = soup.find("bdi").text
            if price:
                book_info["price"] = float(price.replace("£", ""))

        except AttributeError:
            self.logger.warning(f"Could not find price for {url}")
            return None
        
        try:
            # the 2th item always has the rigth image for some reason 
            image = soup.find_all("meta", property="og:image")
            book_info["image"] = image[1]["content"]
        except AttributeError:
            self.logger.warning(f"Could not find image for {url}")

        try:
            stock = soup.find("p", class_="stock in-stock")
        except AttributeError:
            self.logger.warning(f"Could not find stock for {url}")

        book_info["instock"] = stock is not None

        try:
            author = soup.find("th", class_="woocommerce-product-attributes-item__label", string=re.compile("Author"))
            book_info["author"] = author.find_next("td").text.strip()
        except AttributeError:
            self.logger.info(f"Could not find author for {url}")
        
        try:
            publisher = soup.find("th", class_="woocommerce-product-attributes-item__label", string=re.compile("Publisher"))
            book_info["publisher"] = publisher.find_next("td").text.strip()
        except AttributeError:
            self.logger.info(f"Could not find publisher for {url}")

        return book_info

    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/product/' in url