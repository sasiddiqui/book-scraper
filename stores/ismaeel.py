import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper

class IsmaeelScraper(AbstractBookScraper):
    def __init__(self):
        super().__init__('https://ismaeelbooks.co.uk/', "Ismaeel Books", convert_rate=1.3)

    def ignore_url(self, url: str) -> bool:
        ig = [
            "/storage/",
            "/wishlist/",
            "/about/",
            "/contact/",
            "?add-to-cart=",
            "#"
        ]

        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/product/' in url

    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            book_info['title'] = soup.find('h1', class_='entry-title').text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:  

            try:
                book_info["author"] = soup.find("th", text="المؤلف").find_next("a").text.strip()
            except AttributeError:
                pass



            book_info["price"] = soup.find("meta", attrs={"name" : "twitter:data1"})["content"].strip().replace("£", "")

        except AttributeError as e  :
            print(e)
            self.logger.warning(f"Could not find author/publisher/price details on {url}")

        try:
            book_info["image"] = soup.find("img", class_="wp-post-image")["src"]
            book_info["instock"] = "in stock" in soup.find("meta", attrs={"name" : "twitter:data2"})["content"].strip().lower()
            try:
                book_info["publisher"] = soup.find("th", text="الناشر").find_next("a").text.strip()    
            except AttributeError:
                try:
                    book_info["publisher"] = soup.find("th", text="PUBLISHER").find_next("p").text.strip()    
                except AttributeError:
                    pass       

        except AttributeError:
            self.logger.warning(f"Could not find extra book details on {url}")
            return book_info
        
        return book_info
