# python code to scrape AER forthcoming articles
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import sqlite3
import json
from datetime import datetime

def scrape_aer_forthcoming():
    """Scrape articles from AER forthcoming page"""
    url = "https://www.aeaweb.org/journals/aer/forthcoming"
    print(f"Scraping: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    else:
        print(f"Failed to retrieve page: {response.status_code}")
        return None

def extract_article_containers(soup):
    """Extract all article elements from the soup - for AER forthcoming articles"""
    if not soup:
        print("No soup data available")
        return []
    
    # For AER forthcoming articles, try multiple approaches
    # Try list items with class="article" first (as mentioned by user)
    li_articles = soup.find_all('li', class_='article')
    if li_articles:
        print(f"Found {len(li_articles)} <li class='article'> containers")
        return li_articles
    
    # Try any elements with class="article"
    class_articles = soup.find_all(class_='article')
    if class_articles:
        print(f"Found {len(class_articles)} elements with class='article'")
        return class_articles
    
    # Find all article elements (fallback)
    article_containers = soup.find_all('article')
    print(f"Found {len(article_containers)} <article> containers")
    
    return article_containers

def extract_article_data(article_containers):
    """Extract specific data from AER forthcoming article containers"""
    articles_data = []
    
    for container in article_containers:
        article_info = {}
        
        # Extract title - usually in h3 or h2 with class containing 'title'
        title_element = container.find(['h1', 'h2', 'h3'], class_=re.compile(r'title', re.I))
        if not title_element:
            # Fallback: look for any h1-h3 tag
            title_element = container.find(['h1', 'h2', 'h3'])
        
        if title_element:
            title = title_element.get_text(strip=True)
            if title and title != "Manually Added Article Title":
                article_info['title'] = title
            else:
                continue  # Skip if no valid title
        else:
            # Last fallback: try to get first line of text content
            text_content = container.get_text(strip=True)
            if text_content:
                first_line = text_content.split('\n')[0].strip()
                if len(first_line) > 10:  # Assume titles are longer than 10 chars
                    article_info['title'] = first_line
                else:
                    continue  # Skip if no valid title
            else:
                continue  # Skip if no title found
        
        # Extract authors - usually in a div or span with class containing 'author'
        authors_element = container.find(['div', 'span', 'p'], class_=re.compile(r'author', re.I))
        if authors_element:
            article_info['authors'] = authors_element.get_text(strip=True)
        else:
            # Fallback: look for text patterns that might be authors
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines[1:3]:  # Check lines after title
                if ',' in line and len(line) < 200:  # Authors typically have commas
                    article_info['authors'] = line
                    break
        
        # Extract date/publication info
        date_element = container.find(['div', 'span', 'p'], class_=re.compile(r'date|publish', re.I))
        if date_element:
            article_info['date'] = date_element.get_text(strip=True)
        
        # Extract abstract - usually in a div with class containing 'abstract' or 'summary'
        abstract_element = container.find(['div', 'p'], class_=re.compile(r'abstract|summary', re.I))
        if abstract_element:
            article_info['abstract'] = abstract_element.get_text(strip=True)
        else:
            # Fallback: look for longer text content that might be an abstract
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines:
                if len(line) > 100:  # Abstracts are typically longer
                    article_info['abstract'] = line
                    break
        
        # Add volume and issue information for forthcoming articles
        article_info['volume'] = 'forthcoming'
        article_info['issue'] = 'forthcoming'
        
        # Find article links
        article_links = []
        aer_link = None
        
        # First, look for the specific article link in h4 with class="title"
        title_link = container.find('h4', class_='title')
        if title_link:
            title_a = title_link.find('a', href=True)
            if title_a:
                href = title_a.get('href', '')
                if href.startswith('/'):
                    aer_link = f"https://www.aeaweb.org{href}"
                else:
                    aer_link = href
                article_links.append(href)
        
        # If no title link found, look for other links in the container
        if not aer_link:
            links = container.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                article_links.append(href)
                
                # Look for specific article links (not the general forthcoming page)
                if 'aeaweb.org' in href.lower() and 'forthcoming' not in href.lower():
                    if href.startswith('/'):
                        aer_link = f"https://www.aeaweb.org{href}"
                    else:
                        aer_link = href
                # Also look for DOI links
                elif 'doi.org' in href.lower():
                    aer_link = href
        
        if aer_link:
            article_info['aer_link'] = aer_link
        
        if article_links:
            article_info['all_links'] = article_links
        
        # Add debug info
        article_info['container_tag'] = container.name
        article_info['container_class'] = container.get('class', [])
        
        articles_data.append(article_info)
    
    return articles_data

def save_articles_to_csv(articles_data, csv_filename='articles_aer.csv'):
    """Save articles to CSV file, checking for duplicates based on aer_link or title"""
    fieldnames = ['title', 'date', 'authors', 'abstract', 'volume', 'issue', 'aer_link']
    
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
                if row.get('aer_link'):
                    existing_articles.add(row['aer_link'])
                if row.get('title'):
                    existing_titles.add(row['title'].strip())
    
    # Filter out articles that already exist
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        aer_link = article.get('aer_link')
        title = article.get('title', '').strip()
        
        # Check for duplicates based on link or title
        is_duplicate = False
        if aer_link and aer_link in existing_articles:
            is_duplicate = True
        elif title and title in existing_titles:
            is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
            print(f"Duplicate found: {article.get('title', 'Unknown Title')}")
        else:
            new_articles.append(article)
            if aer_link:
                existing_articles.add(aer_link)
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

def save_articles_to_db(articles_data, db_filename='articles_aer.db'):
    """Save articles to SQLite database, checking for duplicates based on aer_link or title"""
    
    # Create output directory structure
    output_dir = 'out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Full path to the database file
    db_filepath = os.path.join(output_dir, db_filename)
    
    # Connect to SQLite database (creates file if it doesn't exist)
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            date TEXT,
            authors TEXT,
            abstract TEXT,
            volume TEXT,
            issue TEXT,
            aer_link TEXT,
            all_links TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(aer_link),
            UNIQUE(title)
        )
    ''')
    
    # Create indexes for faster duplicate checking
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_aer_link ON articles(aer_link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON articles(title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_volume_issue ON articles(volume, issue)')
    
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        aer_link = article.get('aer_link')
        title = article.get('title')
        
        # Check for duplicates based on link or title
        is_duplicate = False
        if aer_link:
            cursor.execute('SELECT id FROM articles WHERE aer_link = ?', (aer_link,))
            if cursor.fetchone():
                is_duplicate = True
        
        if not is_duplicate and title:
            cursor.execute('SELECT id FROM articles WHERE title = ?', (title,))
            if cursor.fetchone():
                is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
            print(f"DB Duplicate found: {article.get('title', 'Unknown Title')}")
            continue
        
        # Prepare data for insertion (use 'forthcoming' for volume and issue)
        insert_data = {
            'title': article.get('title', ''),
            'date': article.get('date', ''),
            'authors': article.get('authors', ''),
            'abstract': article.get('abstract', ''),
            'volume': 'forthcoming',
            'issue': 'forthcoming',
            'aer_link': aer_link,
            'all_links': json.dumps(article.get('all_links', [])),
            'scraped_at': datetime.now().isoformat()
        }
        
        # Insert article into database
        cursor.execute('''
            INSERT INTO articles (title, date, authors, abstract, volume, issue, aer_link, all_links, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            insert_data['title'],
            insert_data['date'],
            insert_data['authors'],
            insert_data['abstract'],
            insert_data['volume'],
            insert_data['issue'],
            insert_data['aer_link'],
            insert_data['all_links'],
            insert_data['scraped_at']
        ))
        
        new_articles.append(article)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    if new_articles:
        print(f"\n💾 Saved {len(new_articles)} new articles to database {db_filepath}")
    else:
        print(f"\n📝 No new articles to save to database {db_filepath}")
    
    if duplicate_count > 0:
        print(f"🔄 DB: Skipped {duplicate_count} duplicate articles")
    
    return len(new_articles), duplicate_count

if __name__ == "__main__":
    # Scrape AER forthcoming articles
    print("Scraping AER forthcoming articles...")
    
    soup = scrape_aer_forthcoming()
    
    if soup:
        # Extract article containers
        article_containers = extract_article_containers(soup)
        
        if article_containers:
            # Extract structured data from containers
            articles_data = extract_article_data(article_containers)
            
            # Save articles to CSV with duplicate checking
            csv_new, csv_dupes = save_articles_to_csv(articles_data)
            
            # Save articles to database with duplicate checking
            db_new, db_dupes = save_articles_to_db(articles_data)
            
            print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
            
            # Display the extracted data
            for i, article in enumerate(articles_data, 1):
                print(f"\n=== Article {i} ===")
                for key, value in article.items():
                    if key not in ['all_links', 'container_tag', 'container_class']:  # Skip debug fields in display
                        print(f"{key.capitalize()}: {value}")
                        
            print(f"\nTotal articles extracted: {len(articles_data)}")
            print(f"New articles saved: {csv_new}")
            print(f"Duplicates skipped: {csv_dupes}")
        else:
            print("No article containers found")
    else:
        print("Failed to scrape the forthcoming webpage")
