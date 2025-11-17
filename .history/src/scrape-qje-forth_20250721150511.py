# python code to scrape QJE forthcoming articles
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime

# Add current directory to path for importing save_db
import sys
sys.path.append(os.path.dirname(__file__))
import save_db

def try_qje_rss_feed():
    """Try to get QJE articles from RSS feed as fallback"""
    rss_url = "https://academic.oup.com/rss/site_5398/3285.xml"
    print(f"Trying QJE RSS feed: {rss_url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    try:
        response = requests.get(rss_url, headers=headers, timeout=15)
        if response.status_code == 200:
            print("✅ RSS feed accessed successfully")
            return parse_qje_rss(response.content)
        else:
            print(f"❌ RSS feed failed: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ RSS feed error: {e}")
        return []

def parse_qje_rss(rss_content):
    """Parse QJE RSS feed content"""
    articles_data = []
    
    try:
        root = ET.fromstring(rss_content)
        items = root.findall('.//item')
        
        print(f"Found {len(items)} items in RSS feed")
        
        for item in items[:10]:  # Limit to latest 10 items
            article_info = {}
            
            # Extract title
            title_elem = item.find('title')
            if title_elem is not None and title_elem.text:
                article_info['title'] = title_elem.text.strip()
            else:
                continue
            
            # Extract link
            link_elem = item.find('link')
            if link_elem is not None and link_elem.text:
                article_info['qje_link'] = link_elem.text.strip()
                article_info['article_link'] = link_elem.text.strip()
            
            # Extract publication date
            pub_date_elem = item.find('pubDate')
            if pub_date_elem is not None and pub_date_elem.text:
                article_info['date'] = pub_date_elem.text.strip()
            
            # Extract description (may contain authors/abstract)
            desc_elem = item.find('description')
            if desc_elem is not None and desc_elem.text:
                description = desc_elem.text.strip()
                # Try to extract authors and abstract from description
                if 'by ' in description.lower():
                    # Simple author extraction
                    parts = description.split('by ', 1)
                    if len(parts) > 1:
                        author_part = parts[1].split('.')[0]  # Take until first period
                        if len(author_part) < 200:  # Reasonable author length
                            article_info['authors'] = author_part.strip()
                
                # Use description as abstract fallback
                if len(description) > 50:
                    article_info['abstract'] = description[:500] + "..." if len(description) > 500 else description
            
            # Add volume and issue for forthcoming
            article_info['volume'] = 'forthcoming'
            article_info['issue'] = 'forthcoming'
            
            articles_data.append(article_info)
        
        print(f"Successfully parsed {len(articles_data)} articles from RSS feed")
        return articles_data
        
    except ET.ParseError as e:
        print(f"❌ Failed to parse RSS XML: {e}")
        return []
    except Exception as e:
        print(f"❌ RSS parsing error: {e}")
        return []

def scrape_qje_forthcoming():
    """Scrape articles from QJE advance articles page"""
    url = "https://academic.oup.com/qje/advance-articles"
    print(f"Scraping: {url}")
    
    # Oxford University Press has strict anti-bot measures
    # Try different approaches to access the content
    
    # Add realistic browser headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
    }
    
    # Use search referer approach since it's working
    print("Using search referer approach...")
    
    # Add search referer to headers
    headers['Referer'] = 'https://google.com'
    
    try:
        # Create session for better handling
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            print(f"✅ Success with search referer")
            
            # Debug: Print some info about the page structure
            print(f"Page title: {soup.title.string if soup.title else 'No title'}")
            print(f"Total divs: {len(soup.find_all('div'))}")
            print(f"Total links: {len(soup.find_all('a'))}")
            
            # Look for common article container patterns
            al_items = soup.find_all(class_=re.compile(r'al-article', re.I))
            if al_items:
                print(f"Found {len(al_items)} elements with 'al-article' in class")
            
            items = soup.find_all(class_=re.compile(r'item', re.I))
            if items:
                print(f"Found {len(items)} elements with 'item' in class")
                
            return soup
        elif response.status_code == 403:
            print(f"❌ 403 Forbidden with search referer")
        else:
            print(f"❌ Status {response.status_code} with search referer")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed with search referer: {e}")
    
    print("\n🚫 QJE Scraping Blocked")
    print("\nFor now, skipping QJE scraping...")
    return None

def extract_article_containers(soup):
    """Extract all article elements from the soup - for QJE advance articles"""
    if not soup:
        print("No soup data available")
        return []
    
    # Focus on the most promising containers: li elements with 'al-article-box' class
    article_boxes = soup.find_all('li', class_=re.compile(r'al-article-box', re.I))
    if article_boxes:
        print(f"Found {len(article_boxes)} li elements with 'al-article-box' class")
        return article_boxes
    
    # Fallback: Look for div elements with 'al-article-items' class
    article_divs = soup.find_all('div', class_=re.compile(r'al-article-items', re.I))
    if article_divs:
        print(f"Found {len(article_divs)} div elements with 'al-article-items' class")
        return article_divs
    
    # Strategy 3: Look for elements with 'al-article' in class (we found 31 of these!)
    al_article_items = soup.find_all(class_=re.compile(r'al-article', re.I))
    if al_article_items:
        print(f"Found {len(al_article_items)} elements with 'al-article' in class")
        return al_article_items
    
    return []

def extract_article_data(article_containers):
    """Extract specific data from QJE advance article containers using the actual HTML structure"""
    articles_data = []
    
    print(f"Processing {len(article_containers)} article containers...")
    
    for container in article_containers:
        article_info = {}
        
        # Extract title - prioritize h5 elements which contain the actual titles
        title_element = container.find('h5')
        if not title_element:
            title_element = container.find(['h1', 'h2', 'h3', 'h4', 'h6'])
        if not title_element:
            title_element = container.find(attrs={'title': True})
        if not title_element:
            # Look for any element with class containing 'title'
            title_element = container.find(class_=re.compile(r'title', re.I))
        
        if title_element:
            title = title_element.get_text(strip=True)
            if title and len(title) > 5:  # Basic validation
                article_info['title'] = title
                print(f"Found title: {title[:80]}...")
            else:
                print(f"Title too short or empty: '{title}'")
                continue  # Skip if no valid title
        else:
            print("No title element found")
            continue  # Skip if no title found
        
        # Extract authors - look for spans or divs that might contain author info
        authors_element = container.find(['div', 'span', 'p'], class_=re.compile(r'author|contrib', re.I))
        if authors_element:
            authors_text = authors_element.get_text(strip=True)
            # Clean up common prefixes
            authors_text = re.sub(r'^(by|author[s]?:?)\s*', '', authors_text, flags=re.I)
            article_info['authors'] = authors_text
            print(f"Found authors: {authors_text[:50]}...")
        else:
            # Look for text patterns that might be authors
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines[1:4]:  # Check lines after title
                # Authors typically have commas and are not too long
                if ',' in line and len(line) < 300 and not any(keyword in line.lower() for keyword in ['published', 'online', 'doi', 'advance', 'abstract']):
                    article_info['authors'] = line
                    print(f"Found authors (fallback): {line[:50]}...")
                    break
        
        # Extract publication date - look for date patterns in text
        text_content = container.get_text()
        date_pattern = r'\b(\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\b'
        date_match = re.search(date_pattern, text_content, re.I)
        if date_match:
            article_info['date'] = date_match.group(1)
            print(f"Found date: {date_match.group(1)}")
        else:
            # Look for any date-like elements
            date_element = container.find(['div', 'span', 'p'], class_=re.compile(r'date|publish', re.I))
            if date_element:
                article_info['date'] = date_element.get_text(strip=True)
                print(f"Found date (element): {date_element.get_text(strip=True)}")
        
        # Extract abstract - look for longer text content
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        for line in lines:
            if len(line) > 100:  # Abstracts are typically longer
                # Skip if it looks like title or author info
                if not any(keyword in line.lower() for keyword in ['published', 'online', 'doi', 'advance']) and line != article_info.get('title', ''):
                    article_info['abstract'] = line
                    print(f"Found abstract: {line[:50]}...")
                    break
        
        # Extract link - look for any links within the container
        qje_link = None
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Look for specific QJE article links
            if '/qje/' in href and ('advance-article' in href or 'article' in href):
                if href.startswith('/'):
                    qje_link = f"https://academic.oup.com{href}"
                else:
                    qje_link = href
                print(f"Found QJE link: {qje_link}")
                break
            # Also look for DOI links
            elif 'doi.org' in href.lower():
                qje_link = href
                print(f"Found DOI link: {qje_link}")
                break
        
        if qje_link:
            article_info['qje_link'] = qje_link
            article_info['article_link'] = qje_link  # Standard field name for database
        
        # Add volume and issue information for advance articles
        article_info['volume'] = 'forthcoming'
        article_info['issue'] = 'forthcoming'
        
        # Only add articles with at least a title
        if 'title' in article_info:
            articles_data.append(article_info)
            print(f"Added article: {article_info['title'][:30]}...")
    
    print(f"Successfully extracted {len(articles_data)} articles")
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
    
    # First try the main webpage
    soup = scrape_qje_forthcoming()
    articles_data = []
    
    if soup:
        # Extract article containers from webpage
        article_containers = extract_article_containers(soup)
        
        if article_containers:
            # Extract structured data from containers
            articles_data = extract_article_data(article_containers)
            print(f"Successfully extracted {len(articles_data)} articles from webpage")
        else:
            print("No article containers found on webpage")
    
    # If webpage failed, try RSS feed as fallback
    if not articles_data:
        print("\n🔄 Webpage scraping failed, trying RSS feed fallback...")
        articles_data = try_qje_rss_feed()
    
    # Process the articles if we have any
    if articles_data:
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
        print("\n❌ Failed to scrape QJE articles from both webpage and RSS feed")
        print("QJE scraping will be skipped for this run")