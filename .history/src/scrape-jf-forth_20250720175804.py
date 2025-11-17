# python code to scrape page
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import sqlite3
import json
from datetime import datetime

# webpage link
#url_jf = "https://afajof.org/forthcoming-articles/"  # Replace with the actual URL

def scrape_jf():
    url = "https://afajof.org/forthcoming-articles/"  # Replace with the actual URL
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Process the soup object as needed
        return soup
    else:
        print(f"Failed to retrieve page: {response.status_code}")
        return None

def extract_article_containers(soup):
    """Extract all article-result-container elements from the soup"""
    if not soup:
        print("No soup data available")
        return []
    
    # Find all elements with class 'article-result-container'
    article_containers = soup.find_all(class_='article-result-container')
    print(f"Found {len(article_containers)} article containers")
    
    return article_containers

def extract_article_data(article_containers):
    """Extract specific data from article containers based on paragraph structure"""
    articles_data = []
    
    for container in article_containers:
        article_info = {}
        
        # Find all paragraphs in the container
        paragraphs = container.find_all('p')
        
        # Extract data based on paragraph order
        if len(paragraphs) >= 1:
            # First paragraph: title
            title = paragraphs[0].get_text(strip=True)
            # Skip articles with manually added titles
            if title == "Manually Added Article Title":
                continue
            article_info['title'] = title
        
        if len(paragraphs) >= 2:
            # Second paragraph: date
            article_info['date'] = paragraphs[1].get_text(strip=True)
        
        if len(paragraphs) >= 3:
            # Third paragraph: authors
            article_info['authors'] = paragraphs[2].get_text(strip=True)
        
        if len(paragraphs) >= 4:
            # Fourth paragraph: abstract
            article_info['abstract'] = paragraphs[3].get_text(strip=True)
        
        # Add volume and issue information for forthcoming articles
        article_info['volume'] = 'forthcoming'
        article_info['issue'] = 'forthcoming'
        
        # Find the div after the paragraphs for the article link
        # Look for divs that contain links
        divs = container.find_all('div')
        article_links = []
        jofi_link = None
        
        for div in divs:
            links = div.find_all('a', href=True)
            if links:
                for link in links:
                    href = link.get('href', '')
                    # Look specifically for links containing 'jofi'
                    if 'jofi' in href.lower():
                        # Extract content inside single quotes from the jofi link
                        match = re.search(r"'([^']*)'", href)
                        if match:
                            doi_id = match.group(1)  # Extract the content inside single quotes
                            jofi_link = f"https://onlinelibrary.wiley.com/doi/{doi_id}"  # Form complete URL
                        else:
                            jofi_link = href  # Fallback to full href if no quotes found
                    article_links.append(href)
        
        if jofi_link:
            article_info['jofi_link'] = jofi_link
        
        if article_links:
            article_info['all_links'] = article_links
        
        # Add debug info about structure
        article_info['paragraph_count'] = len(paragraphs)
        
        articles_data.append(article_info)
    
    return articles_data

def save_articles_to_csv(articles_data, csv_filename='articles_jf.csv'):
    """Save articles to CSV file, checking for duplicates based on jofi_link"""
    fieldnames = ['title', 'date', 'authors', 'abstract', 'volume', 'issue', 'jofi_link']
    
    # Create output directory structure
    output_dir = 'out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Full path to the CSV file
    csv_filepath = os.path.join(output_dir, csv_filename)
    
    # Check if CSV file exists and load existing articles
    existing_articles = set()
    file_exists = os.path.exists(csv_filepath)
    
    if file_exists:
        with open(csv_filepath, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get('jofi_link'):
                    existing_articles.add(row['jofi_link'])
    
    # Filter out articles that already exist
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        jofi_link = article.get('jofi_link')
        if jofi_link and jofi_link in existing_articles:
            duplicate_count += 1
            print(f"Duplicate found: {article.get('title', 'Unknown Title')}")
        else:
            new_articles.append(article)
            if jofi_link:
                existing_articles.add(jofi_link)
    
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

def save_articles_to_db(articles_data, db_filename='articles_jf.db'):
    """Save articles to SQLite database, checking for duplicates based on jofi_link"""
    
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
            jofi_link TEXT UNIQUE,
            all_links TEXT,
            paragraph_count INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create index on jofi_link for faster duplicate checking
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_jofi_link ON articles(jofi_link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_volume_issue ON articles(volume, issue)')
    
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        jofi_link = article.get('jofi_link')
        
        if jofi_link:
            # Check if article already exists
            cursor.execute('SELECT id FROM articles WHERE jofi_link = ?', (jofi_link,))
            if cursor.fetchone():
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
            'jofi_link': jofi_link,
            'all_links': json.dumps(article.get('all_links', [])),
            'paragraph_count': article.get('paragraph_count', 0),
            'scraped_at': datetime.now().isoformat()
        }
        
        # Insert article into database
        cursor.execute('''
            INSERT INTO articles (title, date, authors, abstract, volume, issue, jofi_link, all_links, paragraph_count, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            insert_data['title'],
            insert_data['date'],
            insert_data['authors'],
            insert_data['abstract'],
            insert_data['volume'],
            insert_data['issue'],
            insert_data['jofi_link'],
            insert_data['all_links'],
            insert_data['paragraph_count'],
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
    # Scrape the webpage
    soup = scrape_jf()
    
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
                    if key not in ['all_links', 'paragraph_count']:  # Skip debug fields in display
                        print(f"{key.capitalize()}: {value}")
                    
            print(f"\nTotal articles extracted: {len(articles_data)}")
            print(f"New articles saved: {csv_new}")
            print(f"Duplicates skipped: {csv_dupes}")
        else:
            print("No article containers found")
    else:
        print("Failed to scrape the webpage")
