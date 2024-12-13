import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import SessionLocal
from src.models.models import Restaurant, Tag
from sqlalchemy.orm import Session
from decouple import config
import googlemaps
import openai
import time
import logging
from sqlalchemy import func

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True  # This ensures our configuration takes precedence
)
logger = logging.getLogger(__name__)

# Optionally reduce SQLAlchemy noise while keeping our logs visible
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Initialize API clients
gmaps = googlemaps.Client(key=config('GOOGLE_MAPS_API_KEY'))
openai.api_key = config('OPENAI_API_KEY')

def get_google_maps_tags(place_name: str, location: str) -> list:
    """Get restaurant details from Google Maps API"""
    # Define tags to exclude (generic or uninformative tags)
    excluded_tags = {
        'establishment', 'restaurant', 'food', 'point_of_interest', 
        'business', 'place', 'store', 'meal_takeaway', 'meal_delivery',
        'custom jewelry design',
        'restaurant_reservation', 'wedding bands','jewelry store','engagement rings',
        'jewelry repair', 'gemstone expert', 'luxury watches', 'amsterdam gold district'

    }
    
    # Define or extend this list based on your needs
    relevant_tags = {
        # Cuisines
        'italian', 'japanese', 'chinese', 'indian', 'french', 'mexican', 
        'thai', 'vietnamese', 'korean', 'mediterranean', 'greek', 'spanish',
        'american', 'brazilian', 'middle_eastern', 'asian', 'fusion', 'burger',
        'pizza', 'sushi', 'steak', 'seafood', 'barbecue', 'cafe', 'buffet',
        
        # Dietary
        'vegetarian', 'vegan', 'halal', 'kosher', 'gluten_free',
        
        # Restaurant types
        'fine_dining', 'casual_dining', 'fast_food', 'bistro', 'pizzeria',
        'steakhouse', 'seafood', 'barbecue', 'cafe', 'buffet',
        'gastropub', 'trattoria', 'osteria', 'brasserie', 'bar', 'pub', 'lounge'
    }
    
    try:
        result = gmaps.places(f"{place_name} {location}")
        
        if result['status'] == 'OK':
            place = result['results'][0]
            place_id = place['place_id']
            
            # Get detailed place information with valid fields
            details = gmaps.place(place_id, fields=['price_level', 'type'])
            
            # Collect tags from various sources in the response
            tags = set()
            
            # Filter and add place types
            if 'types' in place:
                # Convert Google's tags to our format (replace underscores with spaces)
                place_tags = {t.replace('_', ' ') for t in place['types']}
                
                # Only keep tags that are in relevant_tags and not in excluded_tags
                filtered_tags = {tag for tag in place_tags 
                               if (tag in relevant_tags or 
                                   tag.replace(' ', '_') in relevant_tags) and 
                                  (tag not in excluded_tags and 
                                   tag.replace(' ', '_') not in excluded_tags)}
                
                tags.update(filtered_tags)
            
            # Add price level if available
            if 'price_level' in details['result']:
                price_map = {1: 'budget friendly', 2: 'moderately priced', 
                           3: 'upscale', 4: 'luxury'}
                if details['result']['price_level'] in price_map:
                    tags.add(price_map[details['result']['price_level']])
            
            return list(tags)
            
    except Exception as e:
        logger.error(f"Error getting Google Maps tags for {place_name}: {str(e)}")
        return []

def get_chatgpt_tags(name: str, location: str, existing_tags: list) -> list:
    """Get restaurant tags using ChatGPT"""
    try:
        prompt = f"""
        For the restaurant "{name}" located in {location}, and knowing it already has these tags: {', '.join(existing_tags)},
        please suggest additional relevant tags. Consider:
        1. Cuisine type (e.g., Italian, Japanese, Fusion)
        2. Dining style (e.g., Fine Dining, Casual, Fast Casual)
        3. Special features (e.g., Rooftop, Waterfront, Historic)
        4. Dietary options (e.g., Vegetarian-Friendly, Vegan Options, Gluten-Free)
        
        Return only the tags as a comma-separated list, without explanations.
        """

        response = openai.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.5
        )

        # Split the response into individual tags and clean them
        tags = [tag.strip().lower() for tag in 
                response.choices[0].message.content.split(',')]
        return tags

    except Exception as e:
        logger.error(f"Error getting ChatGPT tags for {name}: {str(e)}")
        return []

def get_or_create_tag(db: Session, tag_name: str) -> Tag:
    """Get existing tag or create new one"""
    tag = db.query(Tag).filter(Tag.name == tag_name).first()
    if not tag:
        tag = Tag(name=tag_name)
        db.add(tag)
        db.flush()
    return tag

def process_restaurant(db: Session, restaurant: Restaurant):
    """Process a single restaurant and update its tags"""
    logger.info(f"Processing restaurant: {restaurant.name}")
    
    # Get existing tags
    existing_tags = [tag.name for tag in restaurant.tags]
    
    # Get new tags from Google Maps
    gmaps_tags = get_google_maps_tags(restaurant.name, restaurant.location)
    
    # Get additional tags from ChatGPT
    chatgpt_tags = get_chatgpt_tags(restaurant.name, restaurant.location, 
                                   existing_tags + gmaps_tags)
    
    # Combine all tags
    all_tags = set(existing_tags + gmaps_tags + chatgpt_tags)
    print('All tags: ', all_tags)
    

    # Update restaurant tags
    for tag_name in all_tags:
        if tag_name and len(tag_name.strip()) > 0:
            tag = get_or_create_tag(db, tag_name.lower().strip())
            if tag not in restaurant.tags:
                restaurant.tags.append(tag)
    
    try:
        db.commit()
        logger.info(f"Successfully updated tags for {restaurant.name}")
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating tags for {restaurant.name}: {str(e)}")
  

def main():
    """Main function to process all restaurants"""
    db = SessionLocal()
    try:
        # Get only restaurants that don't have any tags
        restaurants = (db.query(Restaurant)
                      .outerjoin(Restaurant.tags)
                      .group_by(Restaurant.id)
                      .having(func.count(Tag.id) == 0)
                      .all())
        
        total = len(restaurants)
        logger.info(f"Found {total} restaurants without tags to process")
        
        for i, restaurant in enumerate(restaurants, 1):
            logger.info(f"Processing {i}/{total}: {restaurant.name}")
            process_restaurant(db, restaurant)
            # Sleep to avoid API rate limits
            time.sleep(2)
            
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    main()