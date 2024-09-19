import asyncio
from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper

async def main():
    # await ZakariyyaBooksScraper().crawl_product_pages()
    await AlBadrBooksScraper().crawl_product_pages()

if __name__ == '__main__':
    # call main
    asyncio.run(main())