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
    if soup:
        # Example: print the title of the page
        print(soup.title.string)
        # You can add more processing logic here
    else:
        print("No data scraped.")