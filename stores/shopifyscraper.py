import requests
from scraper import ScraperError


class ShopifyScraper:
    def __init__(
        self,
        name: str,
        base_url: str,
        storefront_token: str,
        api_version: str = "2025-10",
    ):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Storefront-Access-Token": storefront_token,
        }

    @property
    def graphql_url(self) -> str:
        return f"{self.base_url}/api/{self.api_version}/graphql.json"

    def test_base_url(self):
        response = requests.post(
            self.graphql_url,
            headers=self.headers,
            json={"query": "query { shop { name } }"},
        )
        if response.status_code == 200:
            return True
        raise ScraperError(
            f"{self.name} scraper - Failed to reach {self.base_url}. Status code: {response.status_code}. Skipping scraper..."
        )

    def extract_book_info(self, product: dict) -> dict | None:
        product = product["node"]


        featured_image = product.get("featuredImage")
        author = product.get("author")
        publisher = product.get("publisher")

        book_info = {
            "source": self.name,
            "url": product["onlineStoreUrl"],
            "title": product["title"],
            "author": author["value"] if author else None,
            "publisher": publisher["value"] if publisher else None,
            "price": product["priceRange"]["maxVariantPrice"]["amount"],
            "image": featured_image["url"] if featured_image else None,
            "instock": product["availableForSale"],
            "description": product["description"],
        }

        return book_info

    def is_product_url(self, url: str) -> bool:
        return url.startswith(self.base_url) and "/products/" in url

    async def crawl_product_pages(self, last_crawl_success=None):
        self.test_base_url()

        query = """
            query GetProducts($first: Int!, $after: String) {
                products(first: $first, after: $after) {
                    pageInfo { hasNextPage endCursor }
                    edges {
                        node {
                            id
                            title
                            priceRange { maxVariantPrice { amount } }
                            availableForSale
                            description
                            featuredImage { url }
                            onlineStoreUrl
                            author: metafield(namespace: "custom", key: "author") { value }
                            publisher: metafield(namespace: "custom", key: "publisher") { value }
                        }
                    }
                }
            }
        """

        products = []
        end_cursor = None

        while True:
            variables = {"first": 250, "after": end_cursor}
            response = requests.post(
                self.graphql_url,
                headers=self.headers,
                json={"query": query, "variables": variables},
            ).json()

            products.extend(
                self.extract_book_info(product)
                for product in response["data"]["products"]["edges"]
            )

            page_info = response["data"]["products"]["pageInfo"]
            end_cursor = page_info["endCursor"]
            if not page_info["hasNextPage"]:
                break

        return products
