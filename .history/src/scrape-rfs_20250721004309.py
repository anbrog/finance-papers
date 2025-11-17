# python code to scrape Review of Financial Studies issue pages
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

def scrape_rfs_issue(volume, issue):
    """Scrape articles from a specific RFS volume and issue"""
    url = f"https://academic.oup.com/rfs/issue/{volume}/{issue}"
    print(f"Scraping: {url}")
    
    # Use search referer approach since it works for Oxford Academic
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'Pragma': 'no-cache',
        'Referer': 'https://google.com'
    }
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            print(f"✅ Successfully accessed RFS Volume {volume}, Issue {issue}")
            return soup
        elif response.status_code == 403:
            print(f"❌ 403 Forbidden - Oxford University Press blocking access")
            return None
        else:
            print(f"❌ Failed to retrieve page: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None

def extract_article_containers(soup):
    """Extract all article elements from the soup using RFS-specific classes"""
    if not soup:
        print("No soup data available")
        return []
    
    # Find all elements with class 'al-article-items' (same pattern as QJE)
    article_containers = soup.find_all(class_='al-article-items')
    print(f"Found {len(article_containers)} article containers with 'al-article-items' class")
    
    return article_containers

def extract_issue_date(soup):
    """Extract the issue publication date from the issue info"""
    issue_date = None
    
    # Look for element with 'issue-info-pub' class (same pattern as QJE)
    issue_info_elem = soup.find(class_='issue-info-pub')
    if issue_info_elem:
        issue_text = issue_info_elem.get_text(strip=True)
        # Extract text after the last comma (should be the date like "May 2025")
        if ',' in issue_text:
            date_part = issue_text.split(',')[-1].strip()
            # Validate it looks like a date (month + year)
            if re.match(r'\w+ \d{4}', date_part):
                issue_date = date_part
                print(f"Found issue date: {issue_date}")
    
    return issue_date

def extract_article_data(article_containers, soup, volume, issue):
    """Extract specific data from RFS article containers using RFS-specific CSS classes"""
    articles_data = []
    
    # Extract the issue date once for all articles
    issue_date = extract_issue_date(soup)
    
    for container in article_containers:
        article_info = {}
        
        # Extract title from element with 'at-articleLink' class
        title_element = container.find(class_='at-articleLink')
        if title_element:
            title = title_element.get_text(strip=True)
            if title and len(title) > 5:  # Basic validation
                article_info['title'] = title
            else:
                continue  # Skip if no valid title
        else:
            # Fallback: look for h5 or other heading elements
            title_element = container.find(['h1', 'h2', 'h3', 'h4', 'h5'])
            if title_element:
                title = title_element.get_text(strip=True)
                if title and len(title) > 5:
                    article_info['title'] = title
                else:
                    continue
            else:
                continue  # Skip if no title found
        
        # Extract authors from element with 'al-authors-list' class
        authors_element = container.find(class_='al-authors-list')
        if authors_element:
            authors_text = authors_element.get_text(strip=True)
            # Clean up common prefixes
            authors_text = re.sub(r'^(by|author[s]?:?)\s*', '', authors_text, flags=re.I)
            article_info['authors'] = authors_text
        else:
            # Fallback: look for author-related classes
            authors_element = container.find(['div', 'span', 'p'], class_=re.compile(r'author|contrib', re.I))
            if authors_element:
                authors_text = authors_element.get_text(strip=True)
                authors_text = re.sub(r'^(by|author[s]?:?)\s*', '', authors_text, flags=re.I)
                article_info['authors'] = authors_text
        
        # Use the issue date for all articles in this issue
        if issue_date:
            article_info['date'] = issue_date
        
        # Extract abstract from element with 'chapter-para' class
        abstract_element = container.find(class_='chapter-para')
        if abstract_element:
            abstract_text = abstract_element.get_text(strip=True)
            # Remove common prefixes
            abstract_text = re.sub(r'^(abstract:?)\s*', '', abstract_text, flags=re.I)
            article_info['abstract'] = abstract_text
        else:
            # Fallback: look for abstract-related classes or any paragraph with substantial text
            abstract_element = container.find(['div', 'p'], class_=re.compile(r'abstract|summary', re.I))
            if abstract_element:
                abstract_text = abstract_element.get_text(strip=True)
                abstract_text = re.sub(r'^(abstract:?)\s*', '', abstract_text, flags=re.I)
                article_info['abstract'] = abstract_text
            else:
                # Try to find any substantial text content that might be an abstract
                all_text = container.get_text()
                # Look for longer paragraphs that might be abstracts
                paragraphs = [p.strip() for p in all_text.split('\n') if len(p.strip()) > 100]
                for para in paragraphs:
                    # Skip if it looks like title or author info
                    if not any(skip_word in para.lower() for skip_word in ['published', 'online', 'doi', 'volume', 'issue', article_info.get('title', '').lower()]):
                        article_info['abstract'] = para[:500] + ('...' if len(para) > 500 else '')
                        break
        
        # Add volume and issue information
        article_info['volume'] = volume
        article_info['issue'] = issue
        
        # Extract link from element with 'ww-citation-primary' class
        rfs_link = None
        link_element = container.find(class_='ww-citation-primary')
        if link_element and link_element.get('href'):
            href = link_element.get('href')
            if href.startswith('/'):
                rfs_link = f"https://academic.oup.com{href}"
            else:
                rfs_link = href
        else:
            # Fallback: look for any RFS article links
            links = container.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                # Look for specific RFS article links
                if '/rfs/' in href and ('article' in href or 'doi' in href):
                    if href.startswith('/'):
                        rfs_link = f"https://academic.oup.com{href}"
                    else:
                        rfs_link = href
                    break
                # Also look for DOI links
                elif 'doi.org' in href.lower():
                    rfs_link = href
                    break
        
        if rfs_link:
            article_info['rfs_link'] = rfs_link
            article_info['article_link'] = rfs_link  # Standard field name for database
        
        # Add debug info
        article_info['container_class'] = container.get('class', [])
        
        # Only add articles with at least a title
        if 'title' in article_info:
            articles_data.append(article_info)
    
    return articles_data

def save_articles_to_csv(articles_data, volume, issue, csv_filename='articles_rfs.csv'):
    """Save articles to CSV file, checking for duplicates based on rfs_link or title"""
    fieldnames = ['title', 'date', 'authors', 'abstract', 'volume', 'issue', 'rfs_link']
    
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
                if row.get('rfs_link'):
                    existing_articles.add(row['rfs_link'])
                if row.get('title'):
                    existing_titles.add(row['title'].strip())
    
    # Filter out articles that already exist
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        rfs_link = article.get('rfs_link')
        title = article.get('title', '').strip()
        
        # Check for duplicates based on link or title
        is_duplicate = False
        if rfs_link and rfs_link in existing_articles:
            is_duplicate = True
        elif title and title in existing_titles:
            is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
            print(f"Duplicate found: {article.get('title', 'Unknown Title')}")
        else:
            new_articles.append(article)
            if rfs_link:
                existing_articles.add(rfs_link)
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

def scrape_multiple_issues(volume_issue_pairs):
    """Scrape multiple volume/issue combinations"""
    all_articles = []
    total_new = 0
    total_duplicates = 0
    
    for volume, issue in volume_issue_pairs:
        print(f"\n{'='*50}")
        print(f"Processing RFS Volume {volume}, Issue {issue}")
        print(f"{'='*50}")
        
        # Scrape the issue page
        soup = scrape_rfs_issue(volume, issue)
        
        if soup:
            # Extract article containers
            article_containers = extract_article_containers(soup)
            
            if article_containers:
                # Extract structured data from containers
                articles_data = extract_article_data(article_containers, soup, volume, issue)
                
                # Save articles to CSV with duplicate checking
                csv_new, csv_dupes = save_articles_to_csv(articles_data, volume, issue)
                
                # Save articles to database with duplicate checking
                db_new, db_dupes = save_db.save_articles_to_db(articles_data, 'rfs', volume, issue)
                
                all_articles.extend(articles_data)
                total_new += csv_new
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
        print("Running RFS forthcoming articles scraper...")
        
        # Import and run the forthcoming scraper
        try:
            import subprocess
            result = subprocess.run([
                sys.executable, 
                'src/scrape-rfs-forth.py'
            ], capture_output=True, text=True, cwd='.')
            
            # Print the output from the forthcoming scraper
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
            
            print(f"RFS forthcoming scraper completed with return code: {result.returncode}")
            
        except Exception as e:
            print(f"Error running RFS forthcoming scraper: {e}")
            print("Make sure src/scrape-rfs-forth.py exists in the correct location")
            
    elif len(sys.argv) == 3:
        try:
            VOLUME = int(sys.argv[1])
            ISSUE = int(sys.argv[2])
            print(f"Using command line arguments: Volume {VOLUME}, Issue {ISSUE}")
            
            # Scrape single issue
            print(f"Scraping Review of Financial Studies Volume {VOLUME}, Issue {ISSUE}")
            soup = scrape_rfs_issue(VOLUME, ISSUE)
            
            if soup:
                article_containers = extract_article_containers(soup)
                if article_containers:
                    articles_data = extract_article_data(article_containers, soup, VOLUME, ISSUE)
                    csv_new, csv_dupes = save_articles_to_csv(articles_data, VOLUME, ISSUE)
                    db_new, db_dupes = save_db.save_articles_to_db(articles_data, 'rfs', VOLUME, ISSUE)
                    
                    print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                    
                    # Display only new articles
                    if csv_new > 0:
                        print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
                        new_article_count = 0
                        for article in articles_data:
                            rfs_link = article.get('rfs_link')
                            # Check if this article was actually new (not a duplicate)
                            if rfs_link:  # Only show articles that have valid links (the ones that get saved)
                                new_article_count += 1
                                if new_article_count <= csv_new:  # Only show the number of new articles
                                    print(f"\n=== New Article {new_article_count} ===")
                                    for key, value in article.items():
                                        if key not in ['container_class']:
                                            print(f"{key.capitalize()}: {value}")
                    else:
                        print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
                else:
                    print("No article containers found")
            else:
                print("Failed to scrape the webpage")
                
        except ValueError:
            print("Error: Volume and issue must be integers")
            print("Usage: python src/scrape-rfs.py <volume> [issue]")
            print("       python src/scrape-rfs.py <volume-range>")
            print("       python src/scrape-rfs.py forthcoming")
            print("Example: python src/scrape-rfs.py 38 2        (single issue)")
            print("Example: python src/scrape-rfs.py 38          (all issues 1-12)")
            print("Example: python src/scrape-rfs.py 36-38       (volumes 36, 37, 38 - all issues)")
            print("Example: python src/scrape-rfs.py forthcoming (forthcoming articles)")
            sys.exit(1)
            
    elif len(sys.argv) == 2:
        # Check if it's a volume range (e.g., "36-38")
        if '-' in sys.argv[1] and sys.argv[1] != 'forthcoming':
            try:
                volume_range = sys.argv[1].split('-')
                if len(volume_range) == 2:
                    start_volume = int(volume_range[0])
                    end_volume = int(volume_range[1])
                    
                    if start_volume > end_volume:
                        print("Error: Start volume must be less than or equal to end volume")
                        sys.exit(1)
                    
                    print(f"Using volume range: {start_volume} to {end_volume} (all issues 1-12)")
                    
                    # Create volume-issue pairs for all volumes in range (RFS typically has 12 issues per volume)
                    volume_issue_pairs = []
                    for volume in range(start_volume, end_volume + 1):
                        for issue in range(1, 13):
                            volume_issue_pairs.append((volume, issue))
                    
                    all_articles, total_new, total_duplicates = scrape_multiple_issues(volume_issue_pairs)
                    
                    print(f"\n🎉 Completed scraping RFS Volumes {start_volume}-{end_volume}, all issues")
                    print(f"📊 Total articles processed: {len(all_articles)}")
                    print(f"🆕 Total new articles saved: {total_new}")
                    print(f"🔄 Total duplicates skipped: {total_duplicates}")
                else:
                    raise ValueError("Invalid range format")
                    
            except ValueError:
                print("Error: Volume range must be in format 'start-end' with integers")
                print("Usage: python src/scrape-rfs.py <volume> [issue]")
                print("       python src/scrape-rfs.py <volume-range>")
                print("       python src/scrape-rfs.py forthcoming")
                print("Example: python src/scrape-rfs.py 38 2        (single issue)")
                print("Example: python src/scrape-rfs.py 38          (all issues 1-12)")
                print("Example: python src/scrape-rfs.py 36-38       (volumes 36, 37, 38 - all issues)")
                print("Example: python src/scrape-rfs.py forthcoming (forthcoming articles)")
                sys.exit(1)
        else:
            try:
                VOLUME = int(sys.argv[1])
                print(f"Using command line argument: Volume {VOLUME} (all issues 1-12)")
                
                # Scrape all issues 1-12 for the given volume
                volume_issue_pairs = [(VOLUME, issue) for issue in range(1, 13)]
                all_articles, total_new, total_duplicates = scrape_multiple_issues(volume_issue_pairs)
                
                print(f"\n🎉 Completed scraping RFS Volume {VOLUME}, all issues")
                print(f"📊 Total articles processed: {len(all_articles)}")
                print(f"🆕 Total new articles saved: {total_new}")
                print(f"🔄 Total duplicates skipped: {total_duplicates}")
                
            except ValueError:
                print("Error: Volume must be an integer, range (e.g., '36-38'), or 'forthcoming'")
                print("Usage: python src/scrape-rfs.py <volume> [issue]")
                print("       python src/scrape-rfs.py <volume-range>")
                print("       python src/scrape-rfs.py forthcoming")
                print("Example: python src/scrape-rfs.py 38 2        (single issue)")
                print("Example: python src/scrape-rfs.py 38          (all issues 1-12)")
                print("Example: python src/scrape-rfs.py 36-38       (volumes 36, 37, 38 - all issues)")
                print("Example: python src/scrape-rfs.py forthcoming (forthcoming articles)")
                sys.exit(1)
            
    elif len(sys.argv) == 1:
        # Default values if no arguments provided
        VOLUME = 38
        ISSUE = 1
        print(f"Using default values: Volume {VOLUME}, Issue {ISSUE}")
        print("To specify volume and issue: python src/scrape-rfs.py <volume> [issue]")
        print("To scrape volume range: python src/scrape-rfs.py <volume-range>")
        print("To scrape forthcoming articles: python src/scrape-rfs.py forthcoming")
        
        # Scrape single issue with defaults
        print(f"Scraping Review of Financial Studies Volume {VOLUME}, Issue {ISSUE}")
        soup = scrape_rfs_issue(VOLUME, ISSUE)
        
        if soup:
            article_containers = extract_article_containers(soup)
            if article_containers:
                articles_data = extract_article_data(article_containers, soup, VOLUME, ISSUE)
                csv_new, csv_dupes = save_articles_to_csv(articles_data, VOLUME, ISSUE)
                db_new, db_dupes = save_db.save_articles_to_db(articles_data, 'rfs', VOLUME, ISSUE)
                
                print(f"📄 CSV: {csv_new} new, {csv_dupes} duplicates | 💾 DB: {db_new} new, {db_dupes} duplicates")
                
                # Display only new articles
                if csv_new > 0:
                    print(f"\n🆕 NEW ARTICLES SAVED ({csv_new}):")
                    new_article_count = 0
                    for article in articles_data:
                        rfs_link = article.get('rfs_link')
                        # Check if this article was actually new (not a duplicate)
                        if rfs_link:  # Only show articles that have valid links (the ones that get saved)
                            new_article_count += 1
                            if new_article_count <= csv_new:  # Only show the number of new articles
                                print(f"\n=== New Article {new_article_count} ===")
                                for key, value in article.items():
                                    if key not in ['container_class']:
                                        print(f"{key.capitalize()}: {value}")
                else:
                    print(f"\n📋 No new articles to display (all {len(articles_data)} were duplicates)")
            else:
                print("No article containers found")
        else:
            print("Failed to scrape the webpage")
            
    else:
        print("Error: Invalid number of arguments")
        print("Usage: python src/scrape-rfs.py <volume> [issue]")
        print("       python src/scrape-rfs.py <volume-range>")
        print("       python src/scrape-rfs.py forthcoming")
        print("Examples:")
        print("  python src/scrape-rfs.py 38 2        (scrape Volume 38, Issue 2)")
        print("  python src/scrape-rfs.py 38          (scrape Volume 38, all issues 1-12)")
        print("  python src/scrape-rfs.py 36-38       (scrape Volumes 36, 37, 38 - all issues)")
        print("  python src/scrape-rfs.py forthcoming (scrape forthcoming articles)")
        print("  python src/scrape-rfs.py             (use defaults: Volume 38, Issue 1)")
        sys.exit(1)
