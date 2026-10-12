from stores.assubki import AsSubkiBooks


def _item(**overrides):
    item = {
        "title": "A Comprehensive Guide to Shafi Fiqh",
        "author": "Imam Taqi al Din al Hisni",
        "publisher": "UK Islamic Academy",
        "image": "https://assubkibooks.co.uk/img/uploads/guide.webp",
        "price": 21,
        "url": "https://assubkibooks.co.uk/book/a-comprehensive-guide-to-shafi-fiqh",
        "instock": True,
        "description": "  Writing Al-Fiqh al-Muyassar\nis a simplified abridgement.  ",
    }
    item.update(overrides)
    return item


def test_to_book_maps_fields_and_converts_gbp():
    book = AsSubkiBooks()._to_book(_item())

    assert book.title == "A Comprehensive Guide to Shafi Fiqh"
    assert book.author == "Imam Taqi al Din al Hisni"
    assert book.publisher == "UK Islamic Academy"
    assert book.price == 27.93
    assert book.instock is True
    assert book.source == "As-Subki Books"
    assert book.description == "Writing Al-Fiqh al-Muyassar is a simplified abridgement."
    assert book.image.startswith("https://assubkibooks.co.uk/")


def test_to_book_skips_incomplete_items():
    scraper = AsSubkiBooks()
    assert scraper._to_book(_item(title="  ")) is None
    assert scraper._to_book(_item(url="")) is None
    assert scraper._to_book(_item(price="nope")) is None
