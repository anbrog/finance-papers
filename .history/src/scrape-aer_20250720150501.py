# python code to scrape American Economic Review articles
import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import sys

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

def extract_article_containers(soup, is_forthcoming=False):
    """Extract all article elements from the soup"""
    if not soup:
        print("No soup data available")
        return []
    
    # Find all article elements
    article_containers = soup.find_all('article')
    print(f"Found {len(article_containers)} article containers")
    
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
            article_containers = extract_article_containers(soup, is_forthcoming=False)
            
            if article_containers:
                # Extract structured data from containers
                articles_data = extract_article_data(article_containers, volume=str(issue_id), issue='1')
                
                # Save articles to CSV with duplicate checking
                new_count, duplicate_count = save_articles_to_csv(articles_data, str(issue_id), '1')
                
                all_articles.extend(articles_data)
                total_new += new_count
                total_duplicates += duplicate_count
                
                print(f"Articles extracted from Issue {issue_id}: {len(articles_data)}")
            else:
                print(f"No article containers found for Issue {issue_id}")
        else:
            print(f"Failed to scrape Issue {issue_id}")
    
    return all_articles, total_new, total_duplicates

if __name__ == "__main__":
    # Configuration: Get arguments from command line or use defaults
    
    # Check if command line arguments are provided
    if len(sys.argv) >= 2 and sys.argv[1].lower() in ['forth', 'forthcoming']:
        # Scrape forthcoming articles
        print("Scraping AER forthcoming articles...")
        
        soup = scrape_aer_forthcoming()
        
        if soup:
            # Extract article containers
            article_containers = extract_article_containers(soup, is_forthcoming=True)
            
            if article_containers:
                # Extract structured data from containers
                articles_data = extract_article_data(article_containers, volume='forthcoming', issue='forthcoming')
                
                # Save articles to CSV with duplicate checking
                new_count, duplicate_count = save_articles_to_csv(articles_data, 'forthcoming', 'forthcoming')
                
                # Display the extracted data
                for i, article in enumerate(articles_data, 1):
                    print(f"\n=== Article {i} ===")
                    for key, value in article.items():
                        if key not in ['all_links', 'container_class']:  # Skip debug fields in display
                            print(f"{key.capitalize()}: {value}")
                            
                print(f"\nTotal articles extracted: {len(articles_data)}")
                print(f"New articles saved: {new_count}")
                print(f"Duplicates skipped: {duplicate_count}")
            else:
                print("No article containers found")
        else:
            print("Failed to scrape the forthcoming webpage")
            
    elif len(sys.argv) == 2:
        try:
            ISSUE_ID = int(sys.argv[1])
            print(f"Using command line argument: Issue {ISSUE_ID}")
            
            # Scrape single issue
            print(f"Scraping American Economic Review Issue {ISSUE_ID}")
            soup = scrape_aer_issue(ISSUE_ID)
            
            if soup:
                article_containers = extract_article_containers(soup, is_forthcoming=False)
                if article_containers:
                    articles_data = extract_article_data(article_containers, volume=str(ISSUE_ID), issue='1')
                    new_count, duplicate_count = save_articles_to_csv(articles_data, str(ISSUE_ID), '1')
                    
                    # Display the extracted data
                    for i, article in enumerate(articles_data, 1):
                        print(f"\n=== Article {i} ===")
                        for key, value in article.items():
                            if key not in ['all_links', 'container_class']:
                                print(f"{key.capitalize()}: {value}")
                                
                    print(f"\nTotal articles extracted: {len(articles_data)}")
                    print(f"New articles saved: {new_count}")
                    print(f"Duplicates skipped: {duplicate_count}")
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
            article_containers = extract_article_containers(soup, is_forthcoming=False)
            if article_containers:
                articles_data = extract_article_data(article_containers, volume=str(ISSUE_ID), issue='1')
                new_count, duplicate_count = save_articles_to_csv(articles_data, str(ISSUE_ID), '1')
                
                # Display the extracted data
                for i, article in enumerate(articles_data, 1):
                    print(f"\n=== Article {i} ===")
                    for key, value in article.items():
                        if key not in ['all_links', 'container_class']:
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
        print("Usage: python src/scrape-aer.py [issue_id]")
        print("       python src/scrape-aer.py forthcoming")
        print("Examples:")
        print("  python src/scrape-aer.py 810          (scrape Issue 810)")
        print("  python src/scrape-aer.py forthcoming  (scrape forthcoming articles)")
        print("  python src/scrape-aer.py              (use default: Issue 810)")
        sys.exit(1)
