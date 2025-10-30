# scraper for woo based websites

import requests
from scraper import ScraperError
import logging

logger = logging.getLogger('scraper')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')



class WooScraper:
    def __init__(self, name: str, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.name = name

    def test_base_url(self):
        response = requests.get(f"{self.base_url}/wp-json/wc/v3/products", auth=(self.username, self.password))
        if response.status_code == 200:
            return True
        else:
            raise ScraperError(f"Failed to reach {self.base_url}. Status code: {response.status_code}. Skipping scraper...")

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

        self.test_base_url()

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



