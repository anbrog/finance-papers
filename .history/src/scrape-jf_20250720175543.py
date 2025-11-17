# python code to scrape Journal of Finance issue pages
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import sys
import sqlite3
import json
from datetime import datetime

def scrape_jf_issue(volume, issue):
    """Scrape articles from a specific Journal of Finance volume and issue"""
    url = f"https://afajof.org/issue/volume-{volume}-issue-{issue}/"
    print(f"Scraping: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
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

def extract_article_data(article_containers, volume, issue):
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
        
        # Add volume and issue information
        article_info['volume'] = volume
        article_info['issue'] = issue
        
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

def save_articles_to_csv(articles_data, volume, issue, csv_filename='articles_jf.csv'):
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

def save_articles_to_db(articles_data, volume, issue, db_filename='articles_jf.db'):
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
        
        # Prepare data for insertion
        insert_data = {
            'title': article.get('title', ''),
            'date': article.get('date', ''),
            'authors': article.get('authors', ''),
            'abstract': article.get('abstract', ''),
            'volume': str(volume),
            'issue': str(issue),
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

def scrape_multiple_issues(volume_issue_pairs):
    """Scrape multiple volume/issue combinations"""
    all_articles = []
    total_new = 0
    total_duplicates = 0
    
    for volume, issue in volume_issue_pairs:
        print(f"\n{'='*50}")
        print(f"Processing Volume {volume}, Issue {issue}")
        print(f"{'='*50}")
        
        # Scrape the issue page
        soup = scrape_jf_issue(volume, issue)
        
        if soup:
            # Extract article containers
            article_containers = extract_article_containers(soup)
            
            if article_containers:
                # Extract structured data from containers
                articles_data = extract_article_data(article_containers, volume, issue)
                
                # Save articles to CSV with duplicate checking
                csv_new, csv_dupes = save_articles_to_csv(articles_data, volume, issue)
                
                # Save articles to database with duplicate checking
                db_new, db_dupes = save_articles_to_db(articles_data, volume, issue)
                
                all_articles.extend(articles_data)
                total_new += csv_new  # Use CSV count for total (they should be the same)
                total_duplicates += csv_dupes
                
                print(f"Articles extracted from Vol {volume} Issue {issue}: {len(articles_data)}")
                print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
            else:
                print(f"No article containers found for Vol {volume} Issue {issue}")
        else:
            print(f"Failed to scrape Vol {volume} Issue {issue}")
    
    return all_articles, total_new, total_duplicates

if __name__ == "__main__":
    # Configuration: Get volume and issue from command line arguments or use defaults
    
    # Check if command line arguments are provided
    if len(sys.argv) >= 2 and sys.argv[1].lower() in ['forth', 'forthcoming']:
        # Run the forthcoming articles scraper
        print("Running forthcoming articles scraper...")
        
        # Import and run the forthcoming scraper
        try:
            import subprocess
            result = subprocess.run([
                sys.executable, 
                'src/scrape-jf-forth.py'
            ], capture_output=True, text=True, cwd='.')
            
            # Print the output from the forthcoming scraper
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
            
            print(f"Forthcoming scraper completed with return code: {result.returncode}")
            
        except Exception as e:
            print(f"Error running forthcoming scraper: {e}")
            print("Make sure src/scrape-jf-forth.py exists in the correct location")
            
    elif len(sys.argv) == 3:
        try:
            VOLUME = int(sys.argv[1])
            ISSUE = int(sys.argv[2])
            print(f"Using command line arguments: Volume {VOLUME}, Issue {ISSUE}")
            
            # Scrape single issue
            print(f"Scraping Journal of Finance Volume {VOLUME}, Issue {ISSUE}")
            soup = scrape_jf_issue(VOLUME, ISSUE)
            
            if soup:
                article_containers = extract_article_containers(soup)
                if article_containers:
                    articles_data = extract_article_data(article_containers, VOLUME, ISSUE)
                    csv_new, csv_dupes = save_articles_to_csv(articles_data, VOLUME, ISSUE)
                    db_new, db_dupes = save_articles_to_db(articles_data, VOLUME, ISSUE)
                    
                    print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                    
                    # Display the extracted data
                    for i, article in enumerate(articles_data, 1):
                        print(f"\n=== Article {i} ===")
                        for key, value in article.items():
                            if key not in ['all_links', 'paragraph_count']:
                                print(f"{key.capitalize()}: {value}")
                                
                    print(f"\nTotal articles extracted: {len(articles_data)}")
                    print(f"New articles saved: {csv_new}")
                    print(f"Duplicates skipped: {csv_dupes}")
                else:
                    print("No article containers found")
            else:
                print("Failed to scrape the webpage")
                
        except ValueError:
            print("Error: Volume and issue must be integers")
            print("Usage: python src/scrape-jf.py <volume> [issue]")
            print("       python src/scrape-jf.py <volume-range>")
            print("       python src/scrape-jf.py forthcoming")
            print("Example: python src/scrape-jf.py 80 3        (single issue)")
            print("Example: python src/scrape-jf.py 80          (all issues 1-6)")
            print("Example: python src/scrape-jf.py 78-80       (volumes 78, 79, 80 - all issues)")
            print("Example: python src/scrape-jf.py forthcoming (forthcoming articles)")
            sys.exit(1)
            
    elif len(sys.argv) == 2:
        # Check if it's a volume range (e.g., "78-80")
        if '-' in sys.argv[1] and sys.argv[1] != 'forthcoming':
            try:
                volume_range = sys.argv[1].split('-')
                if len(volume_range) == 2:
                    start_volume = int(volume_range[0])
                    end_volume = int(volume_range[1])
                    
                    if start_volume > end_volume:
                        print("Error: Start volume must be less than or equal to end volume")
                        sys.exit(1)
                    
                    print(f"Using volume range: {start_volume} to {end_volume} (all issues 1-6)")
                    
                    # Create volume-issue pairs for all volumes in range
                    volume_issue_pairs = []
                    for volume in range(start_volume, end_volume + 1):
                        for issue in range(1, 7):
                            volume_issue_pairs.append((volume, issue))
                    
                    all_articles, total_new, total_duplicates = scrape_multiple_issues(volume_issue_pairs)
                    
                    print(f"\n🎉 Completed scraping Volumes {start_volume}-{end_volume}, all issues")
                    print(f"📊 Total articles processed: {len(all_articles)}")
                    print(f"🆕 Total new articles saved: {total_new}")
                    print(f"🔄 Total duplicates skipped: {total_duplicates}")
                else:
                    raise ValueError("Invalid range format")
                    
            except ValueError:
                print("Error: Volume range must be in format 'start-end' with integers")
                print("Usage: python src/scrape-jf.py <volume> [issue]")
                print("       python src/scrape-jf.py <volume-range>")
                print("       python src/scrape-jf.py forthcoming")
                print("Example: python src/scrape-jf.py 80 3        (single issue)")
                print("Example: python src/scrape-jf.py 80          (all issues 1-6)")
                print("Example: python src/scrape-jf.py 78-80       (volumes 78, 79, 80 - all issues)")
                print("Example: python src/scrape-jf.py forthcoming (forthcoming articles)")
                sys.exit(1)
        else:
            try:
                VOLUME = int(sys.argv[1])
                print(f"Using command line argument: Volume {VOLUME} (all issues 1-6)")
                
                # Scrape all issues 1-6 for the given volume
                volume_issue_pairs = [(VOLUME, issue) for issue in range(1, 7)]
                all_articles, total_new, total_duplicates = scrape_multiple_issues(volume_issue_pairs)
                
                print(f"\n🎉 Completed scraping Volume {VOLUME}, all issues")
                print(f"📊 Total articles processed: {len(all_articles)}")
                print(f"🆕 Total new articles saved: {total_new}")
                print(f"🔄 Total duplicates skipped: {total_duplicates}")
                
            except ValueError:
                print("Error: Volume must be an integer, range (e.g., '78-80'), or 'forthcoming'")
                print("Usage: python src/scrape-jf.py <volume> [issue]")
                print("       python src/scrape-jf.py <volume-range>")
                print("       python src/scrape-jf.py forthcoming")
                print("Example: python src/scrape-jf.py 80 3        (single issue)")
                print("Example: python src/scrape-jf.py 80          (all issues 1-6)")
                print("Example: python src/scrape-jf.py 78-80       (volumes 78, 79, 80 - all issues)")
                print("Example: python src/scrape-jf.py forthcoming (forthcoming articles)")
                sys.exit(1)
            
    elif len(sys.argv) == 1:
        # Default values if no arguments provided
        VOLUME = 80
        ISSUE = 4
        print(f"Using default values: Volume {VOLUME}, Issue {ISSUE}")
        print("To specify volume and issue: python src/scrape-jf.py <volume> [issue]")
        print("To scrape volume range: python src/scrape-jf.py <volume-range>")
        print("To scrape forthcoming articles: python src/scrape-jf.py forthcoming")
        
        # Scrape single issue with defaults
        print(f"Scraping Journal of Finance Volume {VOLUME}, Issue {ISSUE}")
        soup = scrape_jf_issue(VOLUME, ISSUE)
        
        if soup:
            article_containers = extract_article_containers(soup)
            if article_containers:
                articles_data = extract_article_data(article_containers, VOLUME, ISSUE)
                csv_new, csv_dupes = save_articles_to_csv(articles_data, VOLUME, ISSUE)
                db_new, db_dupes = save_articles_to_db(articles_data, VOLUME, ISSUE)
                
                print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                
                # Display the extracted data
                for i, article in enumerate(articles_data, 1):
                    print(f"\n=== Article {i} ===")
                    for key, value in article.items():
                        if key not in ['all_links', 'paragraph_count']:
                            print(f"{key.capitalize()}: {value}")
                            
                print(f"\nTotal articles extracted: {len(articles_data)}")
                print(f"New articles saved: {new_count}")
                print(f"Duplicates skipped: {duplicate_count}")
            else:
                print("No article containers found")
        else:
            print("Failed to scrape the webpage")
            
    else:
        print("Error: Invalid number of arguments")
        print("Usage: python src/scrape-jf.py <volume> [issue]")
        print("       python src/scrape-jf.py <volume-range>")
        print("       python src/scrape-jf.py forthcoming")
        print("Examples:")
        print("  python src/scrape-jf.py 80 3        (scrape Volume 80, Issue 3)")
        print("  python src/scrape-jf.py 80          (scrape Volume 80, all issues 1-6)")
        print("  python src/scrape-jf.py 78-80       (scrape Volumes 78, 79, 80 - all issues)")
        print("  python src/scrape-jf.py forthcoming (scrape forthcoming articles)")
        print("  python src/scrape-jf.py             (use defaults: Volume 80, Issue 4)")
        sys.exit(1)