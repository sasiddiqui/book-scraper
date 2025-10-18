#!/usr/bin/env python3
"""
Enhanced scraper with Linux-specific fixes
"""

import requests
import ssl
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class LinuxCompatibleRequests:
    """Wrapper for requests with Linux-specific fixes"""
    
    def __init__(self):
        # Disable SSL warnings for testing
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Create session with retry strategy
        self.session = requests.Session()
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Linux-compatible headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def get(self, url, **kwargs):
        """Enhanced get method with Linux-specific fixes"""
        
        # Default parameters
        params = {
            'timeout': 30,
            'headers': self.headers,
            'proxies': {},  # Disable proxy
            'verify': True,  # Start with SSL verification enabled
        }
        
        # Override with user parameters
        params.update(kwargs)
        
        try:
            # First attempt with SSL verification
            response = self.session.get(url, **params)
            return response
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            print(f"⚠️  SSL/Connection error with {url}: {e}")
            print("🔄 Retrying without SSL verification...")
            
            # Retry without SSL verification
            params['verify'] = False
            try:
                response = self.session.get(url, **params)
                print(f"✅ Success without SSL verification: {response.status_code}")
                return response
            except Exception as e2:
                print(f"❌ Still failed: {e2}")
                raise e2

def test_linux_compatible_requests():
    """Test the Linux-compatible requests wrapper"""
    
    linux_requests = LinuxCompatibleRequests()
    
    test_urls = [
        'https://www.zakariyyabooks.com',
        'https://httpbin.org/get',
        'https://www.google.com',
    ]
    
    print("🧪 Testing Linux-compatible requests wrapper...")
    print("=" * 60)
    
    for url in test_urls:
        print(f"\n🌐 Testing: {url}")
        try:
            response = linux_requests.get(url)
            print(f"✅ Success: {response.status_code}")
            print(f"   Content length: {len(response.content)} bytes")
        except Exception as e:
            print(f"❌ Failed: {e}")

if __name__ == "__main__":
    test_linux_compatible_requests()
