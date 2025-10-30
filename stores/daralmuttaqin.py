import os
from stores.wooscraper import WooScraper

class DarAlMuttaqin(WooScraper):
    def __init__(self):
        super().__init__("Dar Al-Muttaqin", "https://almuttaqin.co.uk", os.getenv("MUTTAQIN_CK"), os.getenv("MUTTAQIN_CS"))
        self.convert_rate = 1.32

    def extract_book_info(self, product: dict) -> dict | None:
        book_info = super().extract_book_info(product)
        author = None
        publisher = None
        for attribute in product["attributes"]:
            if attribute["name"] == "المؤلف":
                author = attribute["options"][0]
            if attribute["name"] == "الناشر":
                publisher = attribute["options"][0]
            

        book_info["price"] = float(book_info["price"]) * self.convert_rate

        book_info["author"] = author
        book_info["publisher"] = publisher
        book_info["description"] = product["description"]

        return book_info