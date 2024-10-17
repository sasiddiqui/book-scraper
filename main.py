import asyncio
from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun
from stores.albalagh import AlBalagh
from stores.kunuz import Kunuz

async def main():
    # await ZakariyyaBooksScraper().crawl_product_pages()
    await Kunuz().crawl_product_pages()

if __name__ == '__main__':
    # call main
    asyncio.run(main())