# python code to scrape page
import requests
from bs4 import BeautifulSoup

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
    """Extract specific data from article containers"""
    articles_data = []
    
    for i, container in enumerate(article_containers):
        article_info = {}
        
        # Debug: Print the HTML structure of the first container
        if i == 0:
            print("\n--- DEBUG: First container HTML structure ---")
            print(container.prettify()[:1000])
            print("--- End of debug info ---\n")
        
        # Try to extract title - look for various possible selectors
        title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        if not title_elem:
            # Try other common title selectors
            title_elem = container.find(class_=['title', 'article-title', 'entry-title'])
        if not title_elem:
            # Try finding any strong or bold text that might be a title
            title_elem = container.find(['strong', 'b'])
        if title_elem:
            article_info['title'] = title_elem.get_text(strip=True)
        
        # Try to extract author information - expand search
        author_elem = container.find(class_=['author', 'authors', 'byline', 'author-name'])
        if not author_elem:
            # Look for text patterns that might indicate authors
            all_text = container.get_text()
            # You might need to adjust this pattern based on the actual format
            if 'by ' in all_text.lower():
                author_elem = container
        if author_elem:
            article_info['authors'] = author_elem.get_text(strip=True)
        
        # Try to extract abstract or description - expand search
        abstract_elem = container.find(class_=['abstract', 'description', 'summary', 'excerpt'])
        if abstract_elem:
            article_info['abstract'] = abstract_elem.get_text(strip=True)
        
        # Extract all text content for manual inspection
        all_text = container.get_text(separator=' ', strip=True)
        article_info['full_text'] = all_text[:300] + "..." if len(all_text) > 300 else all_text
        
        # Look for any links
        links = container.find_all('a', href=True)
        if links:
            article_info['links'] = [link['href'] for link in links]
        
        articles_data.append(article_info)
    
    return articles_data
    
if __name__ == "__main__":
    # Scrape the webpage
    soup = scrape_jf()
    
    if soup:
        # Extract article containers
        article_containers = extract_article_containers(soup)
        
        if article_containers:
            # Extract structured data from containers
            articles_data = extract_article_data(article_containers)
            
            # Display the extracted data
            for i, article in enumerate(articles_data, 1):
                print(f"\n=== Article {i} ===")
                for key, value in article.items():
                    print(f"{key.capitalize()}: {value}")
                    
            print(f"\nTotal articles extracted: {len(articles_data)}")
        else:
            print("No article containers found")
    else:
        print("Failed to scrape the webpage")