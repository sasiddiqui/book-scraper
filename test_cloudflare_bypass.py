#!/usr/bin/env python3
"""
Test script to check if we can bypass Cloudflare using Camoufox
Usage: python test_cloudflare_bypass.py --url <url>
"""

import time
import argparse
from camoufox.sync_api import Camoufox

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
            
            # Take a screenshot for debugging
            try:
                screenshot_path = "/Users/abdullahmohammad/Desktop/book-scraper/cloudflare_test_screenshot.png"
                page.screenshot(path=screenshot_path)
                print(f"📸 Screenshot saved to: {screenshot_path}")
            except Exception as e:
                print(f"⚠️ Could not save screenshot: {e}")
                
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
    
    # Run tests based on selection
    test_cloudflare_bypass(args.url, args.head)
    test_with_turnstile_handling(args.url, args.head)
    
    print("\n" + "=" * 50)
    print("🏁 All tests completed!")