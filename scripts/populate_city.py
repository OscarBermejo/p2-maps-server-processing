import sys
from pathlib import Path
import re
import googlemaps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.models import Restaurant
import urllib.parse
from dotenv import load_dotenv
import os
import logging

# Initialize logger
logger = logging.getLogger(__name__)

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Load environment variables
load_dotenv()

# Get credentials from environment variables
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

# Initialize Google Maps client
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def extract_coordinates_from_url(url):
    """Extract coordinates from Google Maps URL."""
    try:
        # Decode URL-encoded string
        decoded_url = urllib.parse.unquote(url)
        
        # Extract place_id from URL
        place_id_match = re.search(r'place_id:([A-Za-z0-9-_]+)', decoded_url)
        if place_id_match:
            place_id = place_id_match.group(1)
            logger.debug(f"Found place_id: {place_id}")
            # Use Places API to get place details
            place_details = gmaps.place(place_id, fields=['geometry'])
            if place_details and 'result' in place_details:
                location = place_details['result']['geometry']['location']
                logger.info(f"Successfully extracted coordinates from place_id: {location}")
                return location['lat'], location['lng']
        
        # If no place_id found, try the original patterns
        patterns = [
            r'@(-?\d+\.\d+),(-?\d+\.\d+)',
            r'q=(-?\d+\.\d+),(-?\d+\.\d+)',
            r'll=(-?\d+\.\d+),(-?\d+\.\d+)',
            r'place/.*/@(-?\d+\.\d+),(-?\d+\.\d+)',
            r'maps\?.*q=(-?\d+\.\d+),(-?\d+\.\d+)',
            r'dir/.*/@(-?\d+\.\d+),(-?\d+\.\d+)',
            r'search/.*/@(-?\d+\.\d+),(-?\d+\.\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, decoded_url)
            if match:
                return float(match.group(1)), float(match.group(2))
                
        print(f"Debug - URL format not recognized: {decoded_url}")
        
    except Exception as e:
        print(f"Error extracting coordinates from URL: {e}")
    return None

def get_city_from_coordinates(lat, lng):
    """Get city name from coordinates using Google Maps Geocoding API."""
    try:
        result = gmaps.reverse_geocode((lat, lng))
        
        if result:
            # Look for city in address components
            for component in result[0]['address_components']:
                if 'locality' in component['types']:
                    return component['long_name']
                # Fallback to administrative_area_level_2 if locality not found
                elif 'administrative_area_level_2' in component['types']:
                    return component['long_name']
    except Exception as e:
        print(f"Error getting city from coordinates: {e}")
    return None


def update_cities():
    """Update city column for all restaurants."""
    restaurants = session.query(Restaurant).all()
    updated_count = 0
    error_count = 0

    for restaurant in restaurants:
        try:
            if restaurant.location_link:
                coords = extract_coordinates_from_url(restaurant.location_link)
                if coords:
                    lat, lng = coords
                    city = get_city_from_coordinates(lat, lng)
                    
                    if city:
                        restaurant.city = city
                        updated_count += 1
                        print(f"Updated {restaurant.name} with city: {city}")
                    else:
                        error_count += 1
                        print(f"Could not determine city for {restaurant.name}")
                else:
                    error_count += 1
                    print(f"Could not extract coordinates from URL for {restaurant.name}")
            else:
                error_count += 1
                print(f"No location link for {restaurant.name}")
                
        except Exception as e:
            error_count += 1
            print(f"Error processing restaurant {restaurant.name}: {e}")

    # Commit all changes
    try:
        session.commit()
        print(f"\nSummary:")
        print(f"Successfully updated: {updated_count} restaurants")
        print(f"Errors encountered: {error_count} restaurants")
    except Exception as e:
        session.rollback()
        print(f"Error committing changes: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    update_cities()