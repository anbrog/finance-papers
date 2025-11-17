# python code to scrape JFE (Journal of Financial Economics) articles from specific volumes
import requests
from bs4 import BeautifulSoup
import time
import argparse

def test_jfe_access(volume=172):
    """Test access to JFE volume page with different user agents and approaches"""
    url = f"https://www.sciencedirect.com/journal/journal-of-financial-economics/vol/{volume}/"
    print(f"Testing access to JFE Volume {volume}: {url}\n")
    
    # Multiple realistic user agents
    user_agents = [
        # Chrome on macOS (most common academic setup)
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        # Chrome on Windows (university/corporate)
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        # Firefox on macOS
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
        # Safari on macOS
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        # Academic/research institution patterns
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    ]
    
    # Different referers to try
    referers = [
        'https://www.google.com/search?q=journal+financial+economics+sciencedirect',
        'https://scholar.google.com/',
        'https://www.google.com/',
        'https://www.sciencedirect.com/',
        None  # No referer
    ]
    
    success_count = 0
    
    for i, user_agent in enumerate(user_agents):
        browser_name = "Chrome" if "Chrome" in user_agent else "Firefox" if "Firefox" in user_agent else "Safari"
        os_name = "macOS" if "Macintosh" in user_agent else "Windows" if "Windows" in user_agent else "Linux"
        
        print(f"🔍 Testing User Agent {i+1}/{len(user_agents)}: {browser_name} on {os_name}")
        
        for j, referer in enumerate(referers):
            referer_name = referer.split('/')[2] if referer else "None"
            print(f"  📡 Referer {j+1}/{len(referers)}: {referer_name}")
            
            # Build comprehensive headers
            headers = {
                'User-Agent': user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
                'DNT': '1',
            }
            
            if referer:
                headers['Referer'] = referer
            
            # Add browser-specific headers
            if 'Chrome' in user_agent:
                headers.update({
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': f'"{os_name}"',
                })
            
            try:
                session = requests.Session()
                session.headers.update(headers)
                
                response = session.get(url, timeout=30, allow_redirects=True)
                
                print(f"    Status: {response.status_code}")
                print(f"    Content Length: {len(response.text):,} chars")
                
                if response.status_code == 200:
                    # Parse and analyze the content
                    soup = BeautifulSoup(response.content, 'html.parser')
                    title = soup.title.string if soup.title else "No title"
                    
                    print(f"    Page Title: {title[:80]}...")
                    
                    # Look for signs of successful access
                    jfe_indicators = [
                        'journal of financial economics',
                        'volume ' + str(volume),
                        'sciencedirect',
                        'elsevier'
                    ]
                    
                    content_lower = response.text.lower()
                    found_indicators = [ind for ind in jfe_indicators if ind in content_lower]
                    
                    print(f"    Found indicators: {found_indicators}")
                    
                    # Check for article links
                    article_links = soup.find_all('a', href=lambda x: x and '/science/article/pii/' in x)
                    print(f"    Article links found: {len(article_links)}")
                    
                    # Check for blocking signs
                    blocking_signs = ['robot', 'captcha', 'blocked', 'access denied', 'forbidden']
                    found_blocks = [sign for sign in blocking_signs if sign in content_lower]
                    
                    if found_blocks:
                        print(f"    ⚠️  Blocking indicators: {found_blocks}")
                    elif len(found_indicators) >= 2 and len(response.text) > 10000:
                        print(f"    ✅ SUCCESS! Valid JFE page detected")
                        success_count += 1
                        
                        # Save a sample of the HTML for inspection
                        sample_file = f"jfe_volume_{volume}_sample.html"
                        with open(sample_file, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        print(f"    💾 Saved HTML sample to: {sample_file}")
                        
                        return response.text, soup
                    else:
                        print(f"    ❓ Unclear response - might be redirected or partial content")
                        
                elif response.status_code == 403:
                    print(f"    🚫 403 Forbidden - Access denied")
                elif response.status_code == 404:
                    print(f"    ❌ 404 Not Found - Volume {volume} doesn't exist")
                    return None, None
                elif response.status_code == 429:
                    print(f"    ⏰ 429 Too Many Requests - Rate limited")
                    time.sleep(5)
                else:
                    print(f"    ❌ Unexpected status code")
                    
            except requests.exceptions.ConnectionError as e:
                if "Failed to resolve" in str(e) or "nodename nor servname" in str(e):
                    print(f"    🌐 DNS Resolution Error - Check internet connection")
                    print(f"    Error details: {e}")
                    return None, None
                else:
                    print(f"    🔌 Connection Error: {e}")
            except requests.exceptions.Timeout:
                print(f"    ⏱️  Request Timeout")
            except Exception as e:
                print(f"    💥 Unexpected Error: {e}")
            
            print()  # Empty line for readability
            time.sleep(2)  # Brief pause between attempts
    
    print(f"\n📊 Summary: {success_count} successful attempts out of {len(user_agents) * len(referers)} total attempts")
    
    if success_count == 0:
        print("\n💡 Troubleshooting suggestions:")
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

def extract_article_containers(soup):
    """Extract all article elements from the JFE volume page"""
    if not soup:
        print("No soup data available")
        return []
    
    # Look for article containers - JFE uses specific patterns
    # Articles are typically in divs or sections with specific classes
    article_containers = []
    
    # Strategy 1: Look for article links that contain "/science/article/pii/"
    article_links = soup.find_all('a', href=re.compile(r'/science/article/pii/'))
    if article_links:
        print(f"Found {len(article_links)} article links")
        # Get the parent containers of these links
        containers = []
        for link in article_links:
            # Find a suitable parent container that contains the article info
            parent = link.find_parent(['div', 'section', 'article', 'li'])
            if parent and parent not in containers:
                containers.append(parent)
        article_containers = containers
    
    # Strategy 2: Look for elements with classes that might contain articles
    if not article_containers:
        potential_containers = soup.find_all(['div', 'section', 'article'], class_=re.compile(r'article|item|result', re.I))
        if potential_containers:
            print(f"Found {len(potential_containers)} potential article containers")
            article_containers = potential_containers
    
    # Strategy 3: Look for text patterns that indicate articles
    if not article_containers:
        # Find all elements that contain "Article" followed by numbers
        all_elements = soup.find_all(text=re.compile(r'Article\s+\d+', re.I))
        containers = []
        for text_elem in all_elements:
            parent = text_elem.find_parent(['div', 'section', 'article', 'li'])
            if parent and parent not in containers:
                containers.append(parent)
        article_containers = containers
        if containers:
            print(f"Found {len(containers)} containers with 'Article' numbers")
    
    return article_containers

def extract_article_data(article_containers, volume):
    """Extract specific data from JFE article containers"""
    articles_data = []
    
    print(f"Processing {len(article_containers)} article containers...")
    
    for container in article_containers:
        article_info = {}
        
        # Extract title - look for links to individual articles
        title_link = container.find('a', href=re.compile(r'/science/article/pii/'))
        if title_link:
            title = title_link.get_text(strip=True)
            if title and len(title) > 5:  # Basic validation
                article_info['title'] = title
                
                # Extract article URL
                href = title_link.get('href')
                if href:
                    if href.startswith('/'):
                        article_url = f"https://www.sciencedirect.com{href}"
                    else:
                        article_url = href
                    article_info['jfe_link'] = article_url
                    article_info['article_link'] = article_url  # Standard field name for database
            else:
                continue  # Skip if no valid title
        else:
            continue  # Skip if no title link found
        
        # Extract authors - look for text after title that contains author names
        # Authors are typically in text near the title link
        container_text = container.get_text()
        lines = [line.strip() for line in container_text.split('\n') if line.strip()]
        
        # Find the line with the title and look at subsequent lines for authors
        title_found = False
        for i, line in enumerate(lines):
            if title in line:
                title_found = True
                # Look at the next few lines for authors
                for j in range(i+1, min(i+4, len(lines))):
                    author_line = lines[j]
                    # Authors typically have names with commas, not too long, and no special keywords
                    if (',' in author_line or ' and ' in author_line.lower()) and len(author_line) < 200:
                        # Skip lines with article numbers or other metadata
                        if not any(keyword in author_line.lower() for keyword in ['article', 'pdf', 'view', 'download', 'preview', 'vol', 'issue', 'page']):
                            article_info['authors'] = author_line
                            break
                break
        
        # Extract article number - look for "Article XXXXX" pattern
        article_number_match = re.search(r'Article\s+(\d+)', container_text, re.I)
        if article_number_match:
            article_info['article_number'] = article_number_match.group(1)
        
        # Extract publication date - for JFE volumes, we can infer from the volume
        # Typically each volume represents a time period
        current_year = datetime.now().year
        article_info['date'] = f"Volume {volume} ({current_year})"
        
        # Add volume and issue information
        article_info['volume'] = str(volume)
        article_info['issue'] = 'N/A'  # JFE doesn't use issue numbers
        
        # Only add articles with at least a title and link
        if 'title' in article_info and 'jfe_link' in article_info:
            articles_data.append(article_info)
    
    print(f"Successfully extracted {len(articles_data)} articles")
    return articles_data

def save_articles_to_csv(articles_data, csv_filename='articles_jfe.csv'):
    """Save articles to CSV file, checking for duplicates based on jfe_link or title"""
    fieldnames = ['title', 'date', 'authors', 'volume', 'issue', 'article_number', 'jfe_link']
    
    # Create output directory structure
    output_dir = 'out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Full path to the CSV file
    csv_filepath = os.path.join(output_dir, csv_filename)
    
    # Check if CSV file exists and load existing articles
    existing_articles = set()
    existing_titles = set()
    file_exists = os.path.exists(csv_filepath)
    
    if file_exists:
        with open(csv_filepath, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get('jfe_link'):
                    existing_articles.add(row['jfe_link'])
                if row.get('title'):
                    existing_titles.add(row['title'].strip())
    
    # Filter out articles that already exist
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        jfe_link = article.get('jfe_link')
        title = article.get('title', '').strip()
        
        # Check for duplicates based on link or title
        is_duplicate = False
        if jfe_link and jfe_link in existing_articles:
            is_duplicate = True
        elif title and title in existing_titles:
            is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
        else:
            new_articles.append(article)
            if jfe_link:
                existing_articles.add(jfe_link)
            if title:
                existing_titles.add(title)
    
    # Write new articles to CSV
    if new_articles:
        mode = 'a' if file_exists else 'w'
        with open(csv_filepath, mode, encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            for article in new_articles:
                # Only write fields that are in fieldnames
                filtered_article = {key: article.get(key, '') for key in fieldnames}
                writer.writerow(filtered_article)
        
        print(f"\n✅ Saved {len(new_articles)} new articles to {csv_filepath}")
    else:
        print(f"\n📝 No new articles to save to {csv_filepath}")
    
    if duplicate_count > 0:
        print(f"🔄 Skipped {duplicate_count} duplicate articles")
    
    return len(new_articles), duplicate_count

def main():
    """Main function to handle command line arguments and scraping"""
    parser = argparse.ArgumentParser(description='Scrape JFE (Journal of Financial Economics) articles from specific volumes')
    parser.add_argument('volume', nargs='?', default=172, type=int, 
                        help='Volume number to scrape (default: 172)')
    
    args = parser.parse_args()
    volume = args.volume
    
    print(f"Scraping JFE Volume {volume}...")
    
    # Scrape the volume page
    soup = scrape_jfe_volume(volume)
    articles_data = []
    
    if soup:
        # Extract article containers from webpage
        article_containers = extract_article_containers(soup)
        
        if article_containers:
            # Extract structured data from containers
            articles_data = extract_article_data(article_containers, volume)
            print(f"Successfully extracted {len(articles_data)} articles from Volume {volume}")
        else:
            print("No article containers found on webpage")
    
    # Process the articles if we have any
    if articles_data:
        # Save articles to CSV with duplicate checking
        csv_new, csv_dupes = save_articles_to_csv(articles_data)
        
        # Save articles to database with duplicate checking
        db_new, db_dupes = save_db.save_articles_to_db(articles_data, 'jfe', str(volume), 'N/A')
        
        print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
        
        # Display only new articles
        if csv_new > 0:
            print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
            new_article_count = 0
            for article in articles_data:
                jfe_link = article.get('jfe_link')
                # Check if this article was actually new (not a duplicate)
                if jfe_link:  # Only show articles that have valid links (the ones that get saved)
                    new_article_count += 1
                    if new_article_count <= csv_new:  # Only show the number of new articles
                        print(f"\n=== New Article {new_article_count} ===")
                        for key, value in article.items():
                            print(f"{key.capitalize()}: {value}")
        else:
            print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
    else:
        print(f"\n❌ Failed to scrape JFE Volume {volume}")
        print("No articles found or scraping was blocked")

if __name__ == "__main__":
    main()
