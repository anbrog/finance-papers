# python code to scrape JFE (Journal of Financial Economics) articles from specific volumes
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import time
import argparse
from datetime import datetime

# Add current directory to path for importing save_db
import sys
sys.path.append(os.path.dirname(__file__))
import save_db

def scrape_jfe_volume(volume=172):
    """Scrape articles from JFE specific volume page"""
    url = f"https://www.sciencedirect.com/journal/journal-of-financial-economics/vol/{volume}/"
    print(f"Scraping JFE Volume {volume}: {url}")
    
    # Realistic user agents from different browsers and systems
    user_agents = [
        # Chrome on macOS
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        # Chrome on Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        # Firefox on macOS
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
        # Safari on macOS
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        # Edge on Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
        # Academic/institutional access patterns
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    ]
    
    # Try different user agents and approaches
    for i, user_agent in enumerate(user_agents):
        print(f"Attempting with user agent {i+1}/{len(user_agents)}: {user_agent.split(' ')[0]}...")
        
        # Comprehensive headers that match the user agent
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Pragma': 'no-cache',
        }
        
        # Add browser-specific headers
        if 'Chrome' in user_agent:
            headers.update({
                'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"' if 'Macintosh' in user_agent else '"Windows"',
            })
        
        # Add realistic referers
        referers = [
            'https://www.google.com/search?q=journal+financial+economics+sciencedirect',
            'https://scholar.google.com/',
            'https://www.google.com/',
            'https://www.sciencedirect.com/',
            'https://www.elsevier.com/',
        ]
        
        for referer in referers:
            try:
                headers['Referer'] = referer
                print(f"  Trying with referer: {referer.split('/')[2]}...")
                
                # Create session for better state management
                session = requests.Session()
                session.headers.update(headers)
                
                # Add realistic delay between attempts
                time.sleep(2)
                
                response = session.get(url, timeout=45, allow_redirects=True)
                
                if response.status_code == 200:
                    # Validate that we got actual content
                    content_length = len(response.text)
                    if content_length > 5000:  # Reasonable page size
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Check for signs of successful access
                        title = soup.title.string if soup.title else ''
                        if 'journal of financial economics' in title.lower() or 'volume' in title.lower():
                            print(f"✅ Successfully accessed JFE Volume {volume}")
                            return soup
                        else:
                            print(f"  Got response but title doesn't match: {title[:50]}...")
                    else:
                        print(f"  Response too short ({content_length} chars), likely blocked")
                        
                elif response.status_code == 403:
                    print(f"  403 Forbidden - access denied")
                elif response.status_code == 404:
                    print(f"❌ Volume {volume} not found (404)")
                    return None  # Don't try other approaches for 404
                elif response.status_code == 429:
                    print(f"  429 Too Many Requests - rate limited, waiting...")
                    time.sleep(10)
                else:
                    print(f"  Status {response.status_code}")
                    
            except requests.exceptions.ConnectionError as e:
                if "Failed to resolve" in str(e) or "nodename nor servname" in str(e):
                    print(f"❌ DNS resolution failed - check internet connection")
                    print(f"   Error: {e}")
                    return None  # Stop trying if DNS is failing
                else:
                    print(f"  Connection error: {e}")
            except requests.exceptions.Timeout:
                print(f"  Request timeout")
            except requests.exceptions.RequestException as e:
                print(f"  Request failed: {e}")
            except Exception as e:
                print(f"  Unexpected error: {e}")
        
        # Brief pause between user agent attempts
        if i < len(user_agents) - 1:
            time.sleep(3)
    
    print(f"\n❌ Failed to access JFE Volume {volume} with all user agents and referers")
    print("📝 ScienceDirect may be blocking automated access or there may be connectivity issues")
    return None

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
