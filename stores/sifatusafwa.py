from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper

class SifatuSafwa(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.sifatusafwa.com")
        self.name = "sifatusafwa"
    
    def ignore_url(self, url) -> bool:
        ig = [
            "#",
            "SubmitCurrency=",
            "id_currency=",
            ".jpg",
            "/ar/",
            "/fr/",
            "order="
        ]

        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return url.endswith(".html") and "/en/" in url

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = "Sifatu Safwa"

        try:
            title = soup.findAll("dt", text="Title")
            if len(title) > 0:
                book_info["title"] = title[0].find_next_sibling("dd").text
            else:
                title = soup.find("meta", property="og:title")
                book_info["title"] = title["content"]

        except Exception as e:
            print(e)
            self.logger.error(f"Could not find title for {url}")
            return None
        
        try: 
            price = soup.find("meta", property="product:price:amount")
            if price:
                book_info["price"] = float(price["content"])
        except Exception as e:
            print(e)
            self.logger.error(f"Could not find price details on {url}")
        
        try:
            author = soup.findAll("dt", text="Author")
            if len(author) > 0:
                book_info["author"] = author[0].find_next_sibling("dd").text
        except AttributeError as e:
            self.logger.warning(f"Could not find author details on {url}")

    
        try: 
            book_info["description"] = soup.find("meta", property="og:description")["content"]
        except AttributeError as e:
            self.logger.warning(f"Could not find description details on {url}")
            
        
        try: 
            book_info["image"] = soup.find("meta", property="og:image")["content"]
        except AttributeError as e:
            self.logger.warning(f"Could not find image details on {url}")

        try: 
            book_info["instock"] = soup.find("div", class_="product-unavailable") is None
        except AttributeError as e: 
            self.logger.warning(f"Could not find stock details on {url}")
        
        return book_info