from bs4 import BeautifulSoup 
from scraper import AbstractBookScraper
# this scraper in different as it does NOT go to individual product pages and only scrapes the new product page

class Kitaabun(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://kitaabun.com/shopping3/products_new.php")
        self.name = "kitaabun"
        self.batch_size = 5

    
    def ignore_url(self, url) -> bool:
        # only scrap display pages 
        return not self.is_product_url(url)
    
    def add_book(self, book_info: list) -> None:
        self.all_books.extend(book_info)

    def is_product_url(self, url):
        return url.startswith(self.base_url) and "?page=" in url and "action=buy_now" not in url and "products_id" not in url

    def extract_book_info(self, soup: BeautifulSoup, url) -> list | None:
        books = []

        for card in soup.find_all("div", "card p-2"):
            book = {
                "source" : "Kitaabun"
            }

            try:
                book["url"] = card.find("a")["href"]
                book["title"] = card.find("h4").text
            except Exception as e:
                self.logger.error(f"Could not find title for {url}")
                continue
            try:
                # this accound for the sale price as well with split
                book["price"] = float(card.find("h6").text.strip().split("&pound")[-1]) * 1.31
                book["image"] = "https://kitaabun.com/shopping3/" + card.find("img")["src"]
                author = card.find("a", class_="font-weight-bold").text.strip()
                if not ("other" in author.lower() or "Aid" in author or "Motivation" in author):
                    book["author"] = author.replace("Author", "").strip()
                    
            except Exception as e:
                self.logger.error(f"Could not find non essential details on {url}")

            books.append(book)
        return books
        
