import requests
from bs4 import BeautifulSoup
import json
from typing import List, Dict
import time

class MichelinGuideScraper:
    BASE_URL = "https://guide.michelin.com/ie/en/catalunya/barcelona/restaurants"
    
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_restaurant_urls(self, page: int = 1) -> List[str]:
        """
        Get all restaurant URLs from a specific page of Barcelona restaurants
        """
        urls = []
        url = f"{self.BASE_URL}/page/{page}"
        print(f"Fetching URL: {url}")  # Debug log
        
        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            restaurant_cards = soup.find_all('div', class_='card__menu')
            print(f"Found {len(restaurant_cards)} restaurant cards")  # Debug log
            
            for card in restaurant_cards:
                # Try different possible link selectors
                link = (card.find('a', class_='link') or 
                       card.find('a', class_='card__menu-link') or
                       card.find('a'))  # Fallback to any link in the card
                
                if link and 'href' in link.attrs:
                    urls.append('https://guide.michelin.com' + link['href'])
                    print(f"Found URL: {link['href']}")  # Debug each URL found
            
            print(f"Extracted {len(urls)} URLs")  # Debug log
            return urls
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []

    def get_restaurant_by_url(self, url: str) -> Dict:
        """
        Fetch restaurant details from a specific Michelin Guide URL
        """
        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            restaurant = {}
            
            # Extract restaurant name
            name_elem = soup.find('h1')
            restaurant['name'] = name_elem.text.strip() if name_elem else None
            
            # Extract URL
            restaurant['url'] = url
            
            # Extract city from URL
            try:
                parts = url.split('/')
                restaurant_index = parts.index('restaurant')
                if restaurant_index > 0:
                    city = parts[restaurant_index - 1].replace('-', ' ').capitalize()
                    city = city.split('_')[0]
                    restaurant['city'] = city
                else:
                    restaurant['city'] = None
            except (ValueError, IndexError):
                restaurant['city'] = None
            
            # Extract stars and other details from JSON-LD
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    # Extract stars from starRating field
                    star_rating = data.get('starRating', '')
                    if 'Three Stars' in star_rating:
                        restaurant['stars'] = 3
                    elif 'Two Stars' in star_rating:
                        restaurant['stars'] = 2
                    elif 'One Star' in star_rating:
                        restaurant['stars'] = 1
                    else:
                        restaurant['stars'] = 0
                    
                    # Extract address
                    if 'address' in data:
                        restaurant['location'] = data['address'].get('streetAddress')
                    
                    # Extract phone
                    restaurant['telephone'] = data.get('telephone')
                    
                    # Extract cuisine type
                    restaurant['cuisine'] = data.get('servesCuisine')
                    
                except json.JSONDecodeError:
                    restaurant['stars'] = 0
            else:
                restaurant['stars'] = 0
            
            return restaurant
            
        except requests.RequestException as e:
            print(f"Error fetching restaurant: {str(e)}")
            return None

    def scrape_barcelona_restaurants(self) -> List[Dict]:
        """
        Scrape Barcelona restaurants (no limit)
        """
        all_restaurants = []
        page = 1
        
        while True:  # Keep going until no more URLs are found
            print(f"\nFetching Barcelona restaurants page {page}...")
            urls = self.get_restaurant_urls(page)
            
            if not urls:
                print(f"No URLs found on page {page}, stopping...")
                break
                
            for url in urls:
                print(f"\nProcessing restaurant: {url}")
                restaurant = self.get_restaurant_by_url(url)
                if restaurant:
                    print(f"Successfully processed: {restaurant.get('name')}")
                    all_restaurants.append(restaurant)
                else:
                    print(f"Failed to process restaurant at: {url}")
                # Be nice to the server
                time.sleep(2)
            
            page += 1
            
        print(f"\nTotal restaurants processed: {len(all_restaurants)}")
        return all_restaurants

def main():
    scraper = MichelinGuideScraper()
    print("Starting scraper...")
    restaurants = scraper.scrape_barcelona_restaurants()  # Removed max_restaurants parameter
    
    if restaurants:
        # Save results to JSON file
        with open('michelin_restaurants.json', 'w', encoding='utf-8') as f:
            json.dump(restaurants, f, ensure_ascii=False, indent=2)
        
        print(f"Scraped {len(restaurants)} restaurants from Barcelona successfully!")
    else:
        print("Failed to scrape restaurant data")
        print("Check if the website structure has changed or if there are connection issues")

if __name__ == "__main__":
    main()
