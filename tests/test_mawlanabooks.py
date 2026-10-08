from stores.mawlanabooks import MawlanaBooks


def _product(**overrides):
    product = {
        "title": "The Fundamentals of Tawheed: Islamic Monotheism",
        "handle": "the-fundamentals-of-tawheed-islamic-monotheism",
        "onlineStoreUrl": "https://mawlanabooks.com/products/the-fundamentals-of-tawheed-islamic-monotheism",
        "description": "  Tawheed is the core\nof the faith.  ",
        "availableForSale": True,
        "featuredImage": {
            "url": "//cdn.shopify.com/s/files/1/0760/5724/8026/files/TheFundamentals_800x.jpg?v=1"
        },
        "priceRange": {"minVariantPrice": {"amount": "14.95"}},
        "author": {
            "references": {
                "nodes": [
                    {"name": {"value": "Bilal Philips"}},
                    {"name": {"value": "Bilal Philips"}},
                    {"name": {"value": ""}},
                ]
            }
        },
    }
    product.update(overrides)
    return product


def test_product_info_author_price_and_https_image():
    book = MawlanaBooks()._product_info(_product())

    assert book["title"] == "The Fundamentals of Tawheed: Islamic Monotheism"
    assert book["author"] == "Bilal Philips"
    assert book["price"] == 14.95
    assert book["instock"] is True
    assert book["description"] == "Tawheed is the core of the faith."
    assert book["image"].startswith("https://cdn.shopify.com/")
    assert book["image"].endswith("_800x.jpg?v=1")
    assert "publisher" not in book


def test_product_info_joins_authors_and_builds_url():
    book = MawlanaBooks()._product_info(
        _product(
            onlineStoreUrl=None,
            author={
                "references": {
                    "nodes": [
                        {"name": {"value": "Bilal Philips"}},
                        {"name": {"value": "Abu Ameenah"}},
                    ]
                }
            },
            featuredImage=None,
        )
    )

    assert book["author"] == "Bilal Philips, Abu Ameenah"
    assert book["url"] == (
        "https://mawlanabooks.com/products/the-fundamentals-of-tawheed-islamic-monotheism"
    )
    assert book["image"] is None
