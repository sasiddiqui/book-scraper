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
from stores.buraq import Buraq

async def main():

    # await AlBadrBooksScraper().crawl_product_pages()
    # await IsmaeelScraper().crawl_product_pages()
    # await ZakariyyaBooksScraper().crawl_product_pages()
    # await Kunuz().crawl_product_pages()
    # await Qurtuba().crawl_product_pages()
    # await SifatuSafwa().crawl_product_pages()
    # await AlHidayaah().crawl_product_pages()
    # await AlBalagh().crawl_product_pages()
    # await Kitaabun().crawl_product_pages()

    await Buraq().crawl_product_pages()

if __name__ == '__main__':
    # call main
    asyncio.run(main())