# python code to scrape QJE forthcoming articles
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import time
from datetime import datetime

# Add current directory to path for importing save_db
import sys
sys.path.append(os.path.dirname(__file__))
import save_db

def scrape_qje_forthcoming():
    """Scrape articles from QJE advance articles page"""
    url = "https://academic.oup.com/qje/advance-articles"
    print(f"Scraping: {url}")
    
    # Add headers to avoid 403 Forbidden errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Try multiple times with different approaches
    for attempt in range(3):
        try:
            if attempt > 0:
                print(f"Retry attempt {attempt}")
                time.sleep(2)  # Wait between attempts
            
            # Create session for better handling
            session = requests.Session()
            session.headers.update(headers)
            
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                return soup
            elif response.status_code == 403:
                print(f"Access forbidden (403). This website may be blocking automated requests.")
                if attempt == 0:
                    print("Trying alternative approach...")
                    continue
            else:
                print(f"Failed to retrieve page: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < 2:
                continue
    
    print("❌ Unable to access QJE advance articles after multiple attempts.")
    print("This may be due to:")
    print("  - Website blocking automated requests")
    print("  - Temporary server issues")
    print("  - Changes in website access policies")
    print("  - VPN/IP restrictions")
    return None

def extract_article_containers(soup):
    """Extract all article elements from the soup - for QJE advance articles"""
    if not soup:
        print("No soup data available")
        return []
    
    # For QJE advance articles, look for div with class="al-article-items"
    article_items_div = soup.find('div', class_='al-article-items')
    if article_items_div:
        # Find all article containers within this div
        article_containers = article_items_div.find_all(['div', 'article'], class_=re.compile(r'article|item', re.I))
        if article_containers:
            print(f"Found {len(article_containers)} article containers in al-article-items div")
            return article_containers
    
    # Fallback: try to find all elements with class containing 'article'
    class_articles = soup.find_all(class_=re.compile(r'article', re.I))
    if class_articles:
        print(f"Found {len(class_articles)} elements with class containing 'article'")
        return class_articles
    
    # Find all article elements (fallback)
    article_containers = soup.find_all('article')
    print(f"Found {len(article_containers)} <article> containers")
    
    return article_containers

def extract_article_data(article_containers):
    """Extract specific data from QJE advance article containers"""
    articles_data = []
    
    for container in article_containers:
        article_info = {}
        
        # Extract title - usually in h3, h2, or link with title-related class
        title_element = container.find(['h1', 'h2', 'h3', 'a'], class_=re.compile(r'title', re.I))
        if not title_element:
            # Fallback: look for any h1-h3 tag or main link
            title_element = container.find(['h1', 'h2', 'h3'])
            if not title_element:
                title_element = container.find('a', href=re.compile(r'/qje/'))
        
        if title_element:
            title = title_element.get_text(strip=True)
            if title and len(title) > 5:  # Basic validation
                article_info['title'] = title
            else:
                continue  # Skip if no valid title
        else:
            # Last fallback: try to get first meaningful line of text content
            text_content = container.get_text(strip=True)
            if text_content:
                lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                for line in lines:
                    if len(line) > 20 and not line.lower().startswith(('published', 'online', 'advance')):
                        article_info['title'] = line
                        break
                if 'title' not in article_info:
                    continue  # Skip if no valid title
            else:
                continue  # Skip if no title found
        
        # Extract authors - usually in a div or span with class containing 'author' or 'contrib'
        authors_element = container.find(['div', 'span', 'p'], class_=re.compile(r'author|contrib', re.I))
        if authors_element:
            authors_text = authors_element.get_text(strip=True)
            # Clean up common prefixes
            authors_text = re.sub(r'^(by|author[s]?:?)\s*', '', authors_text, flags=re.I)
            article_info['authors'] = authors_text
        else:
            # Fallback: look for text patterns that might be authors
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines[1:4]:  # Check lines after title
                # Authors typically have commas and are not too long
                if ',' in line and len(line) < 300 and not any(keyword in line.lower() for keyword in ['published', 'online', 'doi', 'advance']):
                    article_info['authors'] = line
                    break
        
        # Extract publication date
        date_element = container.find(['div', 'span', 'p'], class_=re.compile(r'date|publish', re.I))
        if date_element:
            article_info['date'] = date_element.get_text(strip=True)
        else:
            # Look for date patterns in text
            text_content = container.get_text()
            date_pattern = r'\b(\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\b'
            date_match = re.search(date_pattern, text_content, re.I)
            if date_match:
                article_info['date'] = date_match.group(1)
        
        # Extract abstract - usually in a div with class containing 'abstract' or 'summary'
        abstract_element = container.find(['div', 'p'], class_=re.compile(r'abstract|summary', re.I))
        if abstract_element:
            abstract_text = abstract_element.get_text(strip=True)
            # Remove common prefixes
            abstract_text = re.sub(r'^(abstract:?)\s*', '', abstract_text, flags=re.I)
            article_info['abstract'] = abstract_text
        else:
            # Fallback: look for longer text content that might be an abstract
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines:
                if len(line) > 150:  # Abstracts are typically longer
                    # Skip if it looks like title or author info
                    if not any(keyword in line.lower() for keyword in ['published', 'online', 'doi', 'advance']):
                        article_info['abstract'] = line
                        break
        
        # Add volume and issue information for advance articles
        article_info['volume'] = 'forthcoming'
        article_info['issue'] = 'forthcoming'
        
        # Find article links
        article_links = []
        qje_link = None
        
        # Look for QJE article links
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            article_links.append(href)
            
            # Look for specific QJE article links
            if '/qje/' in href and '/advance-article/' in href:
                if href.startswith('/'):
                    qje_link = f"https://academic.oup.com{href}"
                else:
                    qje_link = href
            # Also look for DOI links
            elif 'doi.org' in href.lower():
                qje_link = href
        
        if qje_link:
            article_info['qje_link'] = qje_link
            article_info['article_link'] = qje_link  # Standard field name for database
        
        if article_links:
            article_info['all_links'] = article_links
        
        # Add debug info
        article_info['container_tag'] = container.name
        article_info['container_class'] = container.get('class', [])
        
        articles_data.append(article_info)
    
    return articles_data

def save_articles_to_csv(articles_data, csv_filename='articles_qje.csv'):
    """Save articles to CSV file, checking for duplicates based on qje_link or title"""
    fieldnames = ['title', 'date', 'authors', 'abstract', 'volume', 'issue', 'qje_link']
    
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
                if row.get('qje_link'):
                    existing_articles.add(row['qje_link'])
                if row.get('title'):
                    existing_titles.add(row['title'].strip())
    
    # Filter out articles that already exist
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        qje_link = article.get('qje_link')
        title = article.get('title', '').strip()
        
        # Check for duplicates based on link or title
        is_duplicate = False
        if qje_link and qje_link in existing_articles:
            is_duplicate = True
        elif title and title in existing_titles:
            is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
            print(f"Duplicate found: {article.get('title', 'Unknown Title')}")
        else:
            new_articles.append(article)
            if qje_link:
                existing_articles.add(qje_link)
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

if __name__ == "__main__":
    # Scrape QJE advance articles
    print("Scraping QJE advance articles...")
    
    soup = scrape_qje_forthcoming()
    
    if soup:
        # Extract article containers
        article_containers = extract_article_containers(soup)
        
        if article_containers:
            # Extract structured data from containers
            articles_data = extract_article_data(article_containers)
            
            # Save articles to CSV with duplicate checking
            csv_new, csv_dupes = save_articles_to_csv(articles_data)
            
            # Save articles to database with duplicate checking
            db_new, db_dupes = save_db.save_articles_to_db(articles_data, 'qje', 'forthcoming', 'forthcoming')
            
            print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
            
            # Display only new articles
            if csv_new > 0:
                print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
                new_article_count = 0
                for article in articles_data:
                    qje_link = article.get('qje_link')
                    # Check if this article was actually new (not a duplicate)
                    if qje_link:  # Only show articles that have valid links (the ones that get saved)
                        new_article_count += 1
                        if new_article_count <= csv_new:  # Only show the number of new articles
                            print(f"\n=== New Article {new_article_count} ===")
                            for key, value in article.items():
                                if key not in ['all_links', 'container_tag', 'container_class']:  # Skip debug fields in display
                                    print(f"{key.capitalize()}: {value}")
            else:
                print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
        else:
            print("No article containers found")
    else:
        print("Failed to scrape the advance articles webpage")
