from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper

def main():
    ZakariyyaBooksScraper().crawl_product_pages()

if __name__ == '__main__':
    main()