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
    
    for container in article_containers:
        article_info = {}
        
        # Try to extract common elements (adjust based on actual HTML structure)
        title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        if title_elem:
            article_info['title'] = title_elem.get_text(strip=True)
        
        # Look for author information
        author_elem = container.find(class_=['author', 'authors'])
        if author_elem:
            article_info['authors'] = author_elem.get_text(strip=True)
        
        # Look for abstract or description
        abstract_elem = container.find(class_=['abstract', 'description', 'summary'])
        if abstract_elem:
            article_info['abstract'] = abstract_elem.get_text(strip=True)
        
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