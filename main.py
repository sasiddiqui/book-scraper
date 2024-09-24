import asyncio
from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa

async def main():
    # await ZakariyyaBooksScraper().crawl_product_pages()
    await SifatuSafwa().crawl_product_pages()

if __name__ == '__main__':
    # call main
    asyncio.run(main())