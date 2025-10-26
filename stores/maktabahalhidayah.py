import os, requests
from book import Book
import logging

logger = logging.getLogger('scraper')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# This is an integration with woo, not a scraper
class MaktabahAlHidayah():
    def __init__(self) -> None:
        self.name = "Maktabah Al-Hidayah"
        self.base_url = "https://maktabahalhidayah.com"
        self.username = os.getenv("HIDAYAH_CK")
        self.password = os.getenv("HIDAYAH_CS")
    
    def extract_book_info(self, product: dict) -> dict | None:
        """ https://woocommerce.github.io/woocommerce-rest-api-docs/?shell#retrieve-a-product """
        book_info = {
            "source": self.name,
            "url": product["permalink"],
            "title": product["name"],
            "instock": product["stock_status"] == "instock",
            "price" : product["price"],
            "image": product["images"][0]["src"] if len(product["images"]) > 0 else None,
        }
        return book_info
    
    async def crawl_product_pages(self):

        products = []
        page = 1
        while True:

            r = requests.get(f"{self.base_url}/wp-json/wc/v3/products?per_page=100&page={page}", auth=(self.username, self.password)).json()
            if len(r) == 0:
                logger.info(f"No more products found")
                break

            logger.info(f"Found {len(r)} products on page {page}")
            products.extend(r)
            page += 1
        
        return list(filter(lambda x: x is not None, [self.extract_book_info(product) for product in products]))



