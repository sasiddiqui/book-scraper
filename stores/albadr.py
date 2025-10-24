from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper

class AlBadrBooksScraper(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://albadr.co.uk/", "Al-Badr", convert_rate=1.3)
        self.batch_size = 5
    
    def ignore_url(self, url) -> bool:
        ig = [
            "/uploads/",
            "/wishlist/",
            "/about/",
            "/contact/",
            "?",
            "#"
        ]

        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/product/' in url

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("h1", class_="product_title entry-title").text.strip() + " | " + soup.find("div", class_="woocommerce-product-details__short-description").text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None
        
        try: 
            price = soup.find("p", class_="price").text.strip()
            if price:
                price = price.replace("£", "").split(" ")[-1]
                if price.endswith("."):
                    price = price[:-1]

                book_info["price"] = float(price) 

            book_info["image"] = soup.find("img", class_="attachment-shop_single size-shop_single")["src"]
        
        except Exception as e:
            print(e)
            self.logger.warning(f"Could not find essential details on {url}")
            
        
        try: 
            book_info["instock"] = soup.find("p", class_="stock in-stock") is not None
        except AttributeError as e:
            self.logger.warning(f"Could not find extra details on {url}")
        
        return book_info