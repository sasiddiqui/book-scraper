import os

from stores.shopifyscraper import ShopifyScraper


class UmmahSpot(ShopifyScraper):
    def __init__(self):
        super().__init__(
            name="UmmahSpot",
            base_url="https://6nzft5-0j.myshopify.com",
            storefront_token=os.getenv("UMMAHSPOT_TOKEN"),
        )
