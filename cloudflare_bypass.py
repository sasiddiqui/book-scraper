#!/usr/bin/env python3
"""
Test script to check if we can bypass Cloudflare using Camoufox
Usage: python test_cloudflare_bypass.py --url <url>
"""

import time
import argparse
from camoufox.sync_api import Camoufox
import requests

def test_http_with_cookies(url, cookie_dict, headers):
    """Test HTTP requests using cookies from browser session"""
    
    print(f"\n🌐 Testing HTTP requests with session cookies...")
    
    try:
        # Test main URL
        response = requests.get(url, cookies=cookie_dict, headers=headers, timeout=10)
        print(f"✅ Main URL HTTP Request successful!")
        print(f"📊 Status Code: {response.status_code}")
        print(f"📏 Content Length: {len(response.content)} bytes")
        
        # Check if we're still blocked
        if "just a moment" in response.text.lower() or "cloudflare" in response.text.lower() and response.status_code != 200:
            print("❌ Still blocked by Cloudflare in HTTP request")
            return False
        else:
            print("✅ Successfully bypassed Cloudflare with HTTP request!")
            
            # Extract page title
            if "<title>" in response.text.lower():
                title_start = response.text.lower().find("<title>")
                title_end = response.text.lower().find("</title>")
                if title_start != -1 and title_end != -1:
                    title = response.text[title_start+7:title_end]
                    print(f"📄 Page title: {title}")
            
            # Save response for inspection
            with open("/Users/abdullahmohammad/Desktop/book-scraper/http_response.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            print("💾 Response saved to http_response.html")
            
            return True
            
    except requests.exceptions.RequestException as e:
        print(f"❌ HTTP request failed: {e}")
        return False

def test_cloudflare_bypass(url, headless):
    """Test if we can bypass Cloudflare protection on the given URL"""
    
    try:
        print(f"🚀 Starting Camoufox browser for {url}...")
        
        # Initialize Camoufox with stealth options
        with Camoufox(
            window=(1280, 720),
            headless=headless,
            disable_coop=True,  # Helps with Cloudflare Turnstile
            humanize=True  # Enable human-like cursor movement
        ) as browser:
            
            print("🌐 Creating new page...")
            page = browser.new_page()
            
            print(f"🌐 Navigating to {url}...")
            page.goto(url)
            
            # Wait for page to load
            print("⏳ Waiting for page to load...")
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_load_state("networkidle")
            
            # Additional wait for Cloudflare challenge if present
            time.sleep(3)
            
            # Get page information
            title = page.title()
            url = page.url
            
            print(f"📄 Page title: {title}")
            print(f"🔗 Current URL: {url}")
            
            # Check if we're blocked by Cloudflare
            page_content = page.content().lower()
            
            cloudflare_indicators = [
                "cloudflare",
                "checking your browser",
                "ddos protection",
                "ray id",
                "cf-ray",
                "just a moment",
                "please wait",
                "browser verification"
            ]
            
            is_blocked = any(indicator in title.lower() or indicator in page_content for indicator in cloudflare_indicators)
            
            if is_blocked:
                print("❌ BLOCKED: Cloudflare protection detected!")
                print("🔍 Cloudflare indicators found in page content")
                
                # Try to find specific Cloudflare elements
                cf_elements = page.query_selector_all("[data-ray], .cf-browser-verification, #cf-content")
                if cf_elements:
                    print(f"🎯 Found {len(cf_elements)} Cloudflare elements")
                    
            else:
                print("✅ SUCCESS: No Cloudflare protection detected!")
                
                # Try to find some content to verify we can access the site
                links = page.query_selector_all("a")
                images = page.query_selector_all("img")
                
                print(f"🔗 Found {len(links)} links")
                print(f"🖼️ Found {len(images)} images")
                
                # Check if we can find any book-related content
                book_keywords = ["book", "kitab", "islamic", "quran", "hadith", "safina", "ناجت", "كتاب"]
                found_keywords = []
                
                for keyword in book_keywords:
                    if keyword in page_content:
                        found_keywords.append(keyword)
                
                if found_keywords:
                    print(f"📚 Found book-related keywords: {', '.join(found_keywords)}")
                
                # Try to get some text content
                try:
                    body_text = page.query_selector("body")
                    if body_text:
                        text_content = body_text.text_content()[:200]  # First 200 chars
                        print(f"📝 Sample content: {text_content}...")
                except Exception as e:
                    print(f"⚠️ Could not extract text content: {e}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure Camoufox is installed: pip install camoufox")

def test_with_turnstile_handling(url, headless):
    """Test with specific Cloudflare Turnstile handling"""
    
    try:
        print(f"\n🎯 Testing with Cloudflare Turnstile handling on {url}...")
        
        with Camoufox(
            headless=headless,  # Keep visible for Turnstile interaction
            window=(1280, 720),
            disable_coop=True,
            humanize=True  # Enable human-like cursor movement
        ) as browser:
            
            page = browser.new_page()
            page.goto(url)
            
            # Wait for page to load
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_load_state("networkidle")
            
            for i in range(10):
                print("🖱️ Clicking on Turnstile...")
                page.mouse.click(210, 290)
                time.sleep(1)
                if "just a moment" not in page.title().lower():
                    break
                else:
                    print("🔄 Verification failed, retrying...")
                    time.sleep(1)
                
            print(f"📄 Final title: {page.title()}")
            print(f"🔗 Final URL: {page.url}")
            
            # Extract cookies and headers for normal HTTP requests
            print("\n🍪 Extracting session data for HTTP requests...")
            
            # Get cookies from the browser context
            cookies = page.context.cookies()
            print(f"📋 Found {len(cookies)} cookies")
            
            # Convert cookies to requests format
            cookie_dict = {}
            for cookie in cookies:
                cookie_dict[cookie['name']] = cookie['value']
                print(f"  🍪 {cookie['name']}: {cookie['value'][:50]}...")
            
            # Get headers (we'll use common browser headers)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Cookie': "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies]),
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Sec-GPC': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Priority': 'u=0, i',
                'TE': 'trailers'
            }
            print(headers)
            
            # Test HTTP request with cookies using the dedicated function
            http_success = test_http_with_cookies(page.url, cookie_dict, headers)
            if not http_success:
                print("❌ Unable to bypass Cloudflare using cookies")
                return False
            else:
                print("✅ Successfully bypassed Cloudflare using cookies")
                return True
            

            
    except Exception as e:
        print(f"❌ Error in Turnstile test: {e}")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test Cloudflare bypass with Camoufox')
    parser.add_argument('--url', required=True, help='URL to test (e.g., https://example.com)')
    parser.add_argument('--head', action='store_false', default=True, help='Run in headless mode')
    
    args = parser.parse_args()
    
    # Validate URL
    if not args.url.startswith(('http://', 'https://')):
        args.url = 'https://' + args.url
    
    print(f"🧪 Testing Cloudflare bypass with Camoufox")
    print(f"🎯 Target URL: {args.url}")
    print("=" * 50)
    
    # Run browser-based tests
    test_cloudflare_bypass(args.url, args.head)
    test_with_turnstile_handling(args.url, args.head)
    
    print("\n" + "=" * 50)
    print("🏁 All tests completed!")