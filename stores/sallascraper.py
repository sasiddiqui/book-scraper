
import requests


class SallaScraper:
    def __init__(self):
        self.base_url = "https://salla.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def test_base_url(self):
        response = requests.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            return True
        else:
            return False
