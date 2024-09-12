import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scraper import AbstractBookScraper


class ZakariyyaBooksScraper(AbstractBookScraper):
    def __init__(self):
        super().__init__('https://www.zakariyyabooks.com')

    def extract_book_info(self, soup: BeautifulSoup, url):
        book_info = {}

        book_info['URL'] = url

        try:
            book_info['Title'] = soup.find('h1', class_='entry-title').text.strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None

        try:
            book_info['Author'] = soup.find('tr', class_='woocommerce-product-attributes-item--attribute_pa_book-author').find('td').text.strip()
            book_info['Publisher'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_publisher').find('td').text.strip()

            price_container = soup.select('p.price.product-page-price').pop()
            # account for sale price and regular price
            sale_price = price_container.find("ins")
            if sale_price:
                book_info['Price'] = sale_price.text.strip()
            else:
                book_info['Price'] = price_container.find("span", class_="woocommerce-Price-amount amount").text.strip()


        except AttributeError:
            self.logger.warning(f"Could not find author/publisher/price details on {url}")
            return book_info

        try:
            if soup.find("img", class_="wp-post-image ux-skip-lazy").has_attr("src"):
                book_info["Image"] = soup.find("img", class_="wp-post-image ux-skip-lazy")["src"]

            book_info["In Stock"] = soup.find('p', class_='stock out-of-stock') is None
                
            book_info['Editor'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_editor').find('td').text.strip()
            book_info['Edition'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_edition').find('td').text.strip()
            book_info['Year Published'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_year-published').find('td').text.strip()
            book_info['Volumes'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_volumes').find('td').text.strip()
            book_info['Pages'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_pages').find('td').text.strip()
            book_info['Binding'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--attribute_pa_binding').find('td').text.strip()
            book_info['Weight'] = soup.find('tr', class_='woocommerce-product-attributes-item woocommerce-product-attributes-item--weight').find('td').text.strip()


            # TODO breadcrumbs for categories

        except AttributeError:
            self.logger.warning(f"Could not find extra book details on {url}")
            return book_info
        return book_info

    def find_product_links(self, soup):
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(self.base_url, href)
            if self.base_url in absolute_url and '/product/' in absolute_url:
                links.append(absolute_url)
        return links
    
    def is_product_url(self, url):
        return url.startswith(self.base_url) and '/product/' in url

    def ignore_url(self, url) -> bool:
        ig = [
            "wp-content",
            "wishlist",
            "cart",
        ]
        for i in ig:
            if i in url:
                return True
        
        split_url = url.split("/")[-1]
        if "?" in split_url or "#" in split_url:
            return True




# Test parsing
if __name__ == '__main__':
    import requests
    from bs4 import BeautifulSoup

    scraper = ZakariyyaBooksScraper()
    url = 'https://www.zakariyyabooks.com/product/%d8%a7%d9%84%d9%85%d8%af%d9%88%d9%86%d8%a9-%d8%a7%d9%84%d8%ac%d8%a7%d9%85%d8%b9%d8%a9-%d9%84%d9%84%d8%a3%d8%ad%d8%a7%d8%af%d9%8a%d8%ab-%d8%a7%d9%84%d9%85%d8%b1%d9%88%d9%8a%d8%a9-%d8%b9%d9%86-%d8%a7/'

    # url1 = quote('https://www.zakariyyabooks.com/product/شرح-السمارقنديّة-على-الرسالة-العظمى-عليها/', safe=':/')  # Hallucination

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
    else:
        import ipdb; ipdb.set_trace()

    print(scraper.extract_book_info(soup, url))