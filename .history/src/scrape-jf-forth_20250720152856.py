# python code to scrape page
import requests
from bs4 import BeautifulSoup
import csv
import os
import re

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
    """Extract all article elements from the soup - for forthcoming articles, these are list items with class='article'"""
    if not soup:
        print("No soup data available")
        return []
    
    # Debug: Let's check what's actually on the page
    print("=== DEBUGGING: Checking page structure ===")
    
    # Try different approaches to find articles
    print("\n1. Looking for <li class='article'>:")
    li_articles = soup.find_all('li', class_='article')
    print(f"   Found {len(li_articles)} <li class='article'> elements")
    
    print("\n2. Looking for any <li> with 'article' in class:")
    li_with_article = soup.find_all('li', class_=lambda x: x and 'article' in ' '.join(x))
    print(f"   Found {len(li_with_article)} <li> elements with 'article' in class")
    
    print("\n3. Looking for <article> tags:")
    article_tags = soup.find_all('article')
    print(f"   Found {len(article_tags)} <article> elements")
    
    print("\n4. Looking for any element with class='article':")
    any_article_class = soup.find_all(class_='article')
    print(f"   Found {len(any_article_class)} elements with class='article'")
    
    print("\n5. Looking for class='article-result-container' (old method):")
    old_containers = soup.find_all(class_='article-result-container')
    print(f"   Found {len(old_containers)} elements with class='article-result-container'")
    
    # Let's also check for common article-related classes
    print("\n6. Looking for other common article classes:")
    for class_name in ['article-item', 'article-card', 'entry', 'post', 'publication']:
        elements = soup.find_all(class_=class_name)
        if elements:
            print(f"   Found {len(elements)} elements with class='{class_name}'")
    
    # Show a sample of the HTML structure
    print("\n7. Sample of page structure (first 1000 chars):")
    print(str(soup)[:1000] + "..." if len(str(soup)) > 1000 else str(soup))
    
    print("\n=== END DEBUGGING ===\n")
    
    # Try multiple approaches and return the one that works
    if li_articles:
        print("Using <li class='article'> elements")
        return li_articles
    elif li_with_article:
        print("Using <li> elements with 'article' in class")
        return li_with_article
    elif article_tags:
        print("Using <article> tag elements")
        return article_tags
    elif any_article_class:
        print("Using any elements with class='article'")
        return any_article_class
    elif old_containers:
        print("Falling back to old method: class='article-result-container'")
        return old_containers
    else:
        print("No article containers found with any method")
        return []

def extract_article_data(article_containers):
    """Extract specific data from article containers (list items with class='article' for forthcoming articles)"""
    articles_data = []
    
    for container in article_containers:
        article_info = {}
        
        # For forthcoming articles in <li class="article">, the structure is different
        # Look for title in various heading tags or strong elements
        title_element = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong'])
        if not title_element:
            # Fallback: look for the first text content that might be a title
            title_text = container.get_text(strip=True).split('\n')[0] if container.get_text(strip=True) else ""
            if title_text and len(title_text) > 10:  # Assume titles are longer than 10 chars
                article_info['title'] = title_text
        else:
            title = title_element.get_text(strip=True)
            if title and title != "Manually Added Article Title":
                article_info['title'] = title
            else:
                continue  # Skip if no valid title
        
        # Skip if we don't have a title
        if 'title' not in article_info:
            continue
            
        # Look for date information - might be in spans, divs, or text
        date_element = container.find(['span', 'div', 'p'], class_=re.compile(r'date', re.I))
        if date_element:
            article_info['date'] = date_element.get_text(strip=True)
        
        # Look for authors - might be in spans, divs, or after the title
        author_element = container.find(['span', 'div', 'p'], class_=re.compile(r'author', re.I))
        if author_element:
            article_info['authors'] = author_element.get_text(strip=True)
        else:
            # Fallback: look for text patterns that might be authors (names with commas)
            text_content = container.get_text()
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            for line in lines[1:3]:  # Check lines after title
                if ',' in line and len(line) < 200:  # Authors typically have commas and aren't too long
                    article_info['authors'] = line
                    break
        
        # Look for abstract or description
        abstract_element = container.find(['div', 'p'], class_=re.compile(r'abstract|summary|description', re.I))
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
        
        # Find links within the container
        article_links = []
        jofi_link = None
        
        links = container.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Look specifically for links containing 'jofi' or DOI patterns
            if 'jofi' in href.lower() or 'doi.org' in href.lower():
                # Extract content inside single quotes from the jofi link
                match = re.search(r"'([^']*)'", href)
                if match:
                    doi_id = match.group(1)  # Extract the content inside single quotes
                    jofi_link = f"https://onlinelibrary.wiley.com/doi/{doi_id}"  # Form complete URL
                else:
                    jofi_link = href  # Use href as is
            article_links.append(href)
        
        if jofi_link:
            article_info['jofi_link'] = jofi_link
        
        if article_links:
            article_info['all_links'] = article_links
        
        # Add debug info about structure
        article_info['container_tag'] = container.name
        article_info['container_class'] = container.get('class', [])
        
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
            new_count, duplicate_count = save_articles_to_csv(articles_data)
            
            # Display the extracted data
            for i, article in enumerate(articles_data, 1):
                print(f"\n=== Article {i} ===")
                for key, value in article.items():
                    if key not in ['all_links', 'container_tag', 'container_class']:  # Skip debug fields in display
                        print(f"{key.capitalize()}: {value}")
                    
            print(f"\nTotal articles extracted: {len(articles_data)}")
            print(f"New articles saved: {new_count}")
            print(f"Duplicates skipped: {duplicate_count}")
        else:
            print("No article containers found")
    else:
        print("Failed to scrape the webpage")
