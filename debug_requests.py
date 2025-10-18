#!/usr/bin/env python3
"""
Debug script to test requests.get behavior on Linux
Run this on your Ubuntu server to identify the issue
"""

import requests
import socket
import ssl
import urllib3
from urllib.parse import urlparse

def test_dns_resolution(url):
    """Test DNS resolution"""
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        ip = socket.gethostbyname(hostname)
        print(f"✅ DNS Resolution: {hostname} -> {ip}")
        return True
    except Exception as e:
        print(f"❌ DNS Resolution failed: {e}")
        return False

def test_ssl_connection(url):
    """Test SSL connection"""
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
        
        context = ssl.create_default_context()
        with socket.create_connection((hostname, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                print(f"✅ SSL Connection: {hostname}:{port}")
                return True
    except Exception as e:
        print(f"❌ SSL Connection failed: {e}")
        return False

def test_requests_basic(url):
    """Test basic requests.get"""
    try:
        response = requests.get(url, timeout=10)
        print(f"✅ Basic requests.get: Status {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ Basic requests.get failed: {e}")
        return False

def test_requests_with_headers(url):
    """Test requests.get with headers"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"✅ requests.get with headers: Status {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ requests.get with headers failed: {e}")
        return False

def test_requests_no_ssl_verify(url):
    """Test requests.get without SSL verification"""
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(url, verify=False, timeout=10)
        print(f"✅ requests.get without SSL verify: Status {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ requests.get without SSL verify failed: {e}")
        return False

def test_requests_with_proxy_disabled(url):
    """Test requests.get with proxy disabled"""
    try:
        response = requests.get(url, proxies={}, timeout=10)
        print(f"✅ requests.get with proxy disabled: Status {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ requests.get with proxy disabled failed: {e}")
        return False

def main():
    # Test URLs from your scrapers
    test_urls = [
        'https://www.zakariyyabooks.com',
        'https://httpbin.org/get',  # Simple test endpoint
        'https://www.google.com',   # Well-known site
    ]
    
    print("🔍 Testing requests.get behavior on Linux...")
    print("=" * 60)
    
    for url in test_urls:
        print(f"\n🌐 Testing URL: {url}")
        print("-" * 40)
        
        # Test DNS resolution
        dns_ok = test_dns_resolution(url)
        
        # Test SSL connection
        if url.startswith('https'):
            ssl_ok = test_ssl_connection(url)
        
        # Test various requests configurations
        test_requests_basic(url)
        test_requests_with_headers(url)
        test_requests_no_ssl_verify(url)
        test_requests_with_proxy_disabled(url)
        
        print()

if __name__ == "__main__":
    main()
