from scraper import AbstractBookScraper

class AbuHanifah(AbstractBookScraper):

    def __init__(self):
        super().__init__("https://abuhanifahbooks.co.uk/", "Maktabah Abu Hanifah")
        self.batch_size = 5
        self.batch_delay = 1.0  # Increased delay to avoid 429 errors
    
    def ignore_url(self, url):
        ig = [
            "/wishlist/",
            "/about/",
            "/contact/",
            "#",
            "cart",
            "account",
        ]
        return any(i in url for i in ig)
    
    def is_product_url(self, url):
        return '/products/' in url

    def extract_book_info(self, soup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try:
            book_info["title"] = soup.find("meta", property="og:title")["content"].strip()
        except AttributeError:
            self.logger.error(f"Could not find title for {url}")
            return None
        try:
            book_info["price"] = float(soup.find("meta", property="og:price:amount")["content"].strip())
        except AttributeError:
            self.logger.error(f"Could not find price for {url}")
            return None
        
        try:
            book_info["instock"] = soup.find("button", class_="product-form__submit button button--full-width button--secondary").text.strip().lower() == "add to cart"
        except AttributeError:
            self.logger.error(f"Could not find instock for {url}")
            return None
        
        try:
            book_info["image"] = soup.find("meta", property="og:image")["content"].strip()
        except AttributeError:
            self.logger.warning(f"Could not find image for {url}")

        return book_info

