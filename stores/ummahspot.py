import os

import requests
from scraper import ScraperError


class UmmahSpot:
    def __init__(self):
        self.name = "UmmahSpot"
        self.base_url = "https://6nzft5-0j.myshopify.com"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Storefront-Access-Token": os.getenv("UMMAHSPOT_TOKEN"),
        }

    def test_base_url(self):
        response = requests.post(
            f"{self.base_url}/api/2025-10/graphql.json",
            headers=self.headers,
            json={"query": "query { shop { name } }"},
        )
        if response.status_code == 200:
            return True
        else:
            raise ScraperError(
                f"{self.name} scraper - Failed to reach {self.base_url}. Status code: {response.status_code}. Skipping scraper..."
            )
        
    def extract_book_info(self, product: dict) -> dict | None:
        product = product["node"]

        image = None
        author = None
        publisher = None
        if product["featuredImage"] is not None:
            image = product["featuredImage"]["url"]

        if product["author"] is not None:
            author = product["author"]["value"]

        if product["publisher"] is not None:
            publisher = product["publisher"]["value"]


        book_info = {
            "source": self.name,
            "url": product["onlineStoreUrl"],
            "title": product["title"],
            "author": author,
            "publisher": publisher,
            "price": product["priceRange"]["maxVariantPrice"]["amount"],
            "image": image,
            "instock": product["availableForSale"],
            "description": product["description"],
        }
        return book_info
    
    def is_product_url(self, url: str) -> bool:
        return url.startswith(self.base_url) and "/products/" in url

    async def crawl_product_pages(self):

        self.test_base_url()

        products = []
        end_cursor = None
        while True:

            query = "query GetProducts($first: Int!, $after: String) { products(first: $first, after: $after) { pageInfo { hasNextPage endCursor } edges { node { id title priceRange {maxVariantPrice{amount}} availableForSale description featuredImage { url } onlineStoreUrl author: metafield(namespace: \"custom\", key: \"author\") { value } publisher: metafield(namespace: \"custom\", key: \"publisher\") { value } } } } }"
            variables = {"first": 250, "after": end_cursor}
            r = requests.post(f"{self.base_url}/api/2025-10/graphql.json", headers=self.headers, json={"query": query, "variables": variables}).json()

            products.extend(self.extract_book_info(product) for product in r["data"]["products"]["edges"])

            end_cursor = r["data"]["products"]["pageInfo"]["endCursor"]
            if not r["data"]["products"]["pageInfo"]["hasNextPage"]:
                break
        
        return products
