from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper

class Qurtuba(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://qurtubabooks.com/", "Qurtuba", convert_rate=1.3)
    
    def ignore_url(self, url) -> bool:
        ig = [
            "#",
            "?add-to-cart",
            "/uploads/",
        ]

        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/product/' in url

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("h1", class_="product-title product_title entry-title").text.strip()
        except AttributeError:
            self.logger.error(f"Could not find title for {url}")
            return None
        
        try: 
            price_container = soup.select('p.price.product-page-price').pop()
            # account for sale price and regular price
            sale_price = price_container.find("ins")
            if sale_price:
                book_info['price'] = float(sale_price.text.strip().replace("£", ""))
            else:
                book_info['price'] = float(price_container.find("span", class_="woocommerce-Price-amount amount").text.strip().replace("£", ""))


        
        except Exception as e:
            print(e)
            self.logger.error(f"Could not find price details on {url}")
            return None
            
        
        try: 
            book_info["image"] = soup.find("img", class_="wp-post-image ux-skip-lazy")["src"]
        except Exception as e:
            self.logger.warning(f"Could not find image details on {url}")

        try: 
            book_info["instock"] = soup.find("p", class_="stock in-stock") is not None
        except Exception as e:
            self.logger.warning(f"Could not find stock details on {url}")
        
        
        return book_info