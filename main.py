import asyncio
from urllib.parse import urljoin
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun

async def main():
    # await ZakariyyaBooksScraper().crawl_product_pages()
    await Kitaabun().crawl_product_pages([
        "https://kitaabun.com/shopping3/products_new.php" + f"?page={i}" for i in range(1, 278)
    ])

if __name__ == '__main__':
    # call main
    asyncio.run(main())