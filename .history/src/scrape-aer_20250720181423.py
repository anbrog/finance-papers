# python code to scrape American Economic Review articles
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import sys
from datetime import datetime

# Add current directory to path for importing save_db
sys.path.append(os.path.dirname(__file__))
import save_db

def scrape_aer_issue(issue_id=810):
    """Scrape articles from a specific AER issue"""
    url = f"https://www.aeaweb.org/issues/{issue_id}"
    print(f"Scraping: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    else:
        print(f"Failed to retrieve page: {response.status_code}")
        return None

def extract_article_containers(soup):
    """Extract all article elements from the soup"""
    if not soup:
        print("No soup data available")
        return []
    
    # Find all article elements
    article_containers = soup.find_all('article')
    print(f"Found {len(article_containers)} <article> containers")
    
    return article_containers

def extract_article_data(article_containers, volume='unknown', issue='unknown'):
    """Extract specific data from article containers"""
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
            continue  # Skip if no title found
        
        # Extract authors - usually in a div or span with class containing 'author'
        authors_element = container.find(['div', 'span', 'p'], class_=re.compile(r'author', re.I))
        if authors_element:
            article_info['authors'] = authors_element.get_text(strip=True)
        
        # Extract date/publication info
        date_element = container.find(['div', 'span', 'p'], class_=re.compile(r'date|publish', re.I))
        if date_element:
            article_info['date'] = date_element.get_text(strip=True)
        
        # Extract abstract - usually in a div with class containing 'abstract' or 'summary'
        abstract_element = container.find(['div', 'p'], class_=re.compile(r'abstract|summary', re.I))
        if abstract_element:
            article_info['abstract'] = abstract_element.get_text(strip=True)
        
        # Add volume and issue information
        article_info['volume'] = volume
        article_info['issue'] = issue
        
        # Find article links
        article_links = []
        aer_link = None
        
        # Look for links in the container
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Look for AER-specific links (aeaweb.org domain)
            if 'aeaweb.org' in href.lower() or href.startswith('/'):
                if href.startswith('/'):
                    aer_link = f"https://www.aeaweb.org{href}"
                else:
                    aer_link = href
                article_links.append(href)
        
        if aer_link:
            article_info['aer_link'] = aer_link
        
        if article_links:
            article_info['all_links'] = article_links
        
        # Add debug info
        article_info['container_class'] = container.get('class', [])
        
        articles_data.append(article_info)
    
    return articles_data

def save_articles_to_csv(articles_data, volume, issue, csv_filename='articles_aer.csv'):
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

def save_articles_to_db(articles_data, volume, issue, db_filename='articles_aer.db'):
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
        
        # Prepare data for insertion
        insert_data = {
            'title': article.get('title', ''),
            'date': article.get('date', ''),
            'authors': article.get('authors', ''),
            'abstract': article.get('abstract', ''),
            'volume': str(volume),
            'issue': str(issue),
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

def scrape_multiple_issues(issue_ids):
    """Scrape multiple AER issues"""
    all_articles = []
    total_new = 0
    total_duplicates = 0
    
    for issue_id in issue_ids:
        print(f"\n{'='*50}")
        print(f"Processing AER Issue {issue_id}")
        print(f"{'='*50}")
        
        # Scrape the issue page
        soup = scrape_aer_issue(issue_id)
        
        if soup:
            # Extract article containers
            article_containers = extract_article_containers(soup)
            
            if article_containers:
                # Extract structured data from containers
                articles_data = extract_article_data(article_containers, volume=str(issue_id), issue='1')
                
                # Save articles to CSV with duplicate checking
                csv_new, csv_dupes = save_articles_to_csv(articles_data, str(issue_id), '1')
                
                # Save articles to database with duplicate checking
                db_new, db_dupes = save_articles_to_db(articles_data, str(issue_id), '1')
                
                all_articles.extend(articles_data)
                total_new += csv_new
                total_duplicates += csv_dupes
                
                print(f"Articles extracted from Issue {issue_id}: {len(articles_data)}")
                print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
            else:
                print(f"No article containers found for Issue {issue_id}")
        else:
            print(f"Failed to scrape Issue {issue_id}")
    
    return all_articles, total_new, total_duplicates

if __name__ == "__main__":
    # Configuration: Get arguments from command line or use defaults
    
    # Check if command line arguments are provided
    if len(sys.argv) >= 2 and sys.argv[1].lower() in ['forth', 'forthcoming']:
        # Run the forthcoming articles scraper
        print("Running AER forthcoming articles scraper...")
        
        # Import and run the forthcoming scraper
        try:
            import subprocess
            result = subprocess.run([
                sys.executable, 
                'src/scrape-aer-forth.py'
            ], capture_output=True, text=True, cwd='.')
            
            # Print the output from the forthcoming scraper
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
            
            print(f"AER forthcoming scraper completed with return code: {result.returncode}")
            
        except Exception as e:
            print(f"Error running AER forthcoming scraper: {e}")
            print("Make sure src/scrape-aer-forth.py exists in the correct location")
            
    elif len(sys.argv) == 2:
        try:
            ISSUE_ID = int(sys.argv[1])
            print(f"Using command line argument: Issue {ISSUE_ID}")
            
            # Scrape single issue
            print(f"Scraping American Economic Review Issue {ISSUE_ID}")
            soup = scrape_aer_issue(ISSUE_ID)
            
            if soup:
                article_containers = extract_article_containers(soup)
                if article_containers:
                    articles_data = extract_article_data(article_containers, volume=str(ISSUE_ID), issue='1')
                    csv_new, csv_dupes = save_articles_to_csv(articles_data, str(ISSUE_ID), '1')
                    db_new, db_dupes = save_articles_to_db(articles_data, str(ISSUE_ID), '1')
                    
                    print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                    
                    # Display only new articles
                    if csv_new > 0:
                        print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
                        new_article_count = 0
                        for article in articles_data:
                            aer_link = article.get('aer_link')
                            # Check if this article was actually new (not a duplicate)
                            if aer_link:  # Only show articles that have valid links (the ones that get saved)
                                new_article_count += 1
                                if new_article_count <= csv_new:  # Only show the number of new articles
                                    print(f"\n=== New Article {new_article_count} ===")
                                    for key, value in article.items():
                                        if key not in ['all_links', 'container_class']:
                                            print(f"{key.capitalize()}: {value}")
                    else:
                        print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
                else:
                    print("No article containers found")
            else:
                print("Failed to scrape the webpage")
                
        except ValueError:
            print("Error: Issue ID must be an integer or 'forthcoming'")
            print("Usage: python src/scrape-aer.py [issue_id]")
            print("       python src/scrape-aer.py forthcoming")
            print("Example: python src/scrape-aer.py 810          (specific issue)")
            print("Example: python src/scrape-aer.py forthcoming  (forthcoming articles)")
            print("Example: python src/scrape-aer.py              (default: issue 810)")
            sys.exit(1)
            
    elif len(sys.argv) == 1:
        # Default values if no arguments provided
        ISSUE_ID = 810
        print(f"Using default value: Issue {ISSUE_ID}")
        print("To specify issue: python src/scrape-aer.py <issue_id>")
        print("To scrape forthcoming articles: python src/scrape-aer.py forthcoming")
        
        # Scrape single issue with defaults
        print(f"Scraping American Economic Review Issue {ISSUE_ID}")
        soup = scrape_aer_issue(ISSUE_ID)
        
        if soup:
            article_containers = extract_article_containers(soup)
            if article_containers:
                articles_data = extract_article_data(article_containers, volume=str(ISSUE_ID), issue='1')
                csv_new, csv_dupes = save_articles_to_csv(articles_data, str(ISSUE_ID), '1')
                db_new, db_dupes = save_articles_to_db(articles_data, str(ISSUE_ID), '1')
                
                print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                
                # Display only new articles
                if csv_new > 0:
                    print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
                    new_article_count = 0
                    for article in articles_data:
                        aer_link = article.get('aer_link')
                        # Check if this article was actually new (not a duplicate)
                        if aer_link:  # Only show articles that have valid links (the ones that get saved)
                            new_article_count += 1
                            if new_article_count <= csv_new:  # Only show the number of new articles
                                print(f"\n=== New Article {new_article_count} ===")
                                for key, value in article.items():
                                    if key not in ['all_links', 'container_class']:
                                        print(f"{key.capitalize()}: {value}")
                else:
                    print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
            else:
                print("No article containers found")
        else:
            print("Failed to scrape the webpage")
            
    else:
        print("Error: Invalid number of arguments")
        print("Usage: python src/scrape-aer.py [issue_id]")
        print("       python src/scrape-aer.py forthcoming")
        print("Examples:")
        print("  python src/scrape-aer.py 810          (scrape Issue 810)")
        print("  python src/scrape-aer.py forthcoming  (scrape forthcoming articles)")
        print("  python src/scrape-aer.py              (use default: Issue 810)")
        sys.exit(1)
