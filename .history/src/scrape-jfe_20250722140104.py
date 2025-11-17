# python code to test access to JFE (Journal of Financial Economics) volume pages
import requests
from bs4 import BeautifulSoup
import time
import argparse

def test_jfe_access(volume=172):
    """Test access to JFE volume page with specific Safari user agent"""
    url = f"https://www.sciencedirect.com/journal/journal-of-financial-economics/vol/{volume}/"
    print(f"Testing access to JFE Volume {volume}: {url}\n")
    
    # Use the specific Safari user agent provided
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15'
    
    print(f"🔍 Testing with Safari 18.5 on macOS")
    
    # Build Safari-specific headers
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'DNT': '1',
    }
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=30, allow_redirects=True)
        
        print(f"  Status: {response.status_code}")
        print(f"  Content Length: {len(response.text):,} chars")
        
        if response.status_code == 200:
            # Parse and analyze the content
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.title.string if soup.title else "No title"
            
            print(f"  Page Title: {title[:80]}...")
            
            # Look for signs of successful access
            jfe_indicators = [
                'journal of financial economics',
                'volume ' + str(volume),
                'sciencedirect',
                'elsevier'
            ]
            
            content_lower = response.text.lower()
            found_indicators = [ind for ind in jfe_indicators if ind in content_lower]
            
            print(f"  Found indicators: {found_indicators}")
            
            # Check for article links
            article_links = soup.find_all('a', href=lambda x: x and '/science/article/pii/' in x)
            print(f"  Article links found: {len(article_links)}")
            
            # Check for blocking signs
            blocking_signs = ['robot', 'captcha', 'blocked', 'access denied', 'forbidden']
            found_blocks = [sign for sign in blocking_signs if sign in content_lower]
            
            if found_blocks:
                print(f"  ⚠️  Blocking indicators: {found_blocks}")
            elif len(found_indicators) >= 2 and len(response.text) > 10000:
                print(f"  ✅ SUCCESS! Valid JFE page detected")
                
                # Save a sample of the HTML for inspection
                sample_file = f"jfe_volume_{volume}_sample.html"
                with open(sample_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print(f"  💾 Saved HTML sample to: {sample_file}")
                
                return response.text, soup
            else:
                print(f"  ❓ Unclear response - might be redirected or partial content")
                
        elif response.status_code == 403:
            print(f"  🚫 403 Forbidden - Access denied")
        elif response.status_code == 404:
            print(f"  ❌ 404 Not Found - Volume {volume} doesn't exist")
            return None, None
        elif response.status_code == 429:
            print(f"  ⏰ 429 Too Many Requests - Rate limited")
        else:
            print(f"  ❌ Unexpected status code")
            
    except requests.exceptions.ConnectionError as e:
        if "Failed to resolve" in str(e) or "nodename nor servname" in str(e):
            print(f"  🌐 DNS Resolution Error - Check internet connection")
            print(f"  Error details: {e}")
            return None, None
        else:
            print(f"  🔌 Connection Error: {e}")
    except requests.exceptions.Timeout:
        print(f"  ⏱️  Request Timeout")
    except Exception as e:
        print(f"  💥 Unexpected Error: {e}")
    
    print(f"\n Troubleshooting suggestions:")
    print("   1. Check your internet connection")
    print("   2. Try accessing the URL manually in a browser")
    print("   3. Check if you're behind a corporate firewall")
    print("   4. ScienceDirect may be blocking automated access")
    print(f"   5. Manual URL: {url}")
    
    return None, None

def main():
    """Main function to test JFE access"""
    parser = argparse.ArgumentParser(description='Test access to JFE (Journal of Financial Economics) volume pages')
    parser.add_argument('volume', nargs='?', default=172, type=int, 
                        help='Volume number to test (default: 172)')
    
    args = parser.parse_args()
    volume = args.volume
    
    print(f"🧪 JFE Access Test - Volume {volume}")
    print("=" * 50)
    
    html_content, soup = test_jfe_access(volume)
    
    if html_content:
        print(f"\n✅ Successfully retrieved HTML content ({len(html_content):,} characters)")
        print("🎉 JFE scraping should work with this configuration!")
    else:
        print(f"\n❌ Unable to retrieve JFE Volume {volume}")
        print("🔧 Consider using alternative data sources or manual access")

if __name__ == "__main__":
    main()
