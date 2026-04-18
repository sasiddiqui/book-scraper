import os
from stores.shopifyscraper import ShopifyScraper

class Kunuz(ShopifyScraper):
    def __init__(self):
        super().__init__(
            name="Kunuz",
            base_url="https://13ryhn-zk.myshopify.com",
            storefront_token=os.getenv("AL_KUNUZ_TOKEN"),
        )