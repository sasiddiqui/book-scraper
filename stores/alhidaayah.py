from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper

class AlHidayaah(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.al-hidaayah.co.uk/collections/all", "Al-Hidayaah")
        self.headers["Accept-Language"] = "en-US,en;q=0.5"
        self.batch_size = 8
    
    def ignore_url(self, url) -> bool:
        ig = [
            "#"
        ]

        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/products/' in url

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("h1", class_="product-meta__title heading h1").text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None
        
        try: 
            price = soup.find("meta", attrs={"property" : "product:price:amount"})["content"]
            book_info["price"] = float(price)
        
        except Exception as e:
            print(e)
            self.logger.warning(f"Could not find price details on {url}")
            
        
        try: 
            book_info["image"] = soup.find("meta", attrs={"name" : "twitter:image"})["content"]
        except AttributeError as e:
            self.logger.warning(f"Could not find image details on {url}")

        try: 
            book_info["instock"] = soup.find("button", class_="product-form__add-button button button--disabled") is None
        except AttributeError as e:
            self.logger.warning(f"Could not find stock details on {url}")

        try: 
            book_info["description"] = soup.find("meta", attrs={"name" : "twitter:description"})["content"].strip().replace("\n", "")
        except AttributeError as e:
            self.logger.warning(f"Could not find description on {url}")

        
        return book_info