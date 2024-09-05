import asyncio
from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper

async def main():
    await ZakariyyaBooksScraper().crawl_product_pages()

if __name__ == '__main__':
    # call main
    asyncio.run(main())