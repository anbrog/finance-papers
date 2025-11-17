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
    
if __name__ == "__main__":
    soup = scrape_jf()
    # Extract all article-result-container elements
    if soup:
        # Find all elements with class 'article-result-container'
        article_containers = soup.find_all(class_='article-result-container')
        
        print(f"Found {len(article_containers)} article containers")
        
        # Loop through each container and extract information
        for i, container in enumerate(article_containers, 1):
            print(f"\n--- Article {i} ---")
            print(container.prettify()[:500] + "..." if len(container.prettify()) > 500 else container.prettify())
    else:
        print("No soup data available")