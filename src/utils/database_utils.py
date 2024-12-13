from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Optional
from datetime import datetime
from src.models.models import Restaurant, Video
from src.database import get_db

def extract_city_from_address(address: str) -> str:
    """Extract city from address string."""
    # Split address by commas and clean up whitespace
    parts = [part.strip() for part in address.split(',')]
    
    # For addresses like "Street, Postal Code City, Country"
    # The city is typically the second-to-last part before the country
    if len(parts) >= 2:
        city_part = parts[-2]  # Get the part before the country
        # Extract just the city name if it includes postal code
        city = city_part.split()[-1]  # Get the last word which is typically the city
        return city
    return "Unknown"

def update_database(
    video_id: str,
    platform: str,
    video_url: str,
    creator_info: Dict[str, str],
    places_data: Dict[str, Dict]
) -> None:
    """
    Update database with video and restaurant information
    
    Args:
        video_id: The ID of the processed video
        platform: The platform the video is from (e.g., "tiktok")
        video_url: The original URL of the video
        creator_info: Dictionary containing creator information
        places_data: Dictionary containing restaurant information from Google Maps
    """
    db = next(get_db())
    
    try:
        # First, create restaurant entries
        restaurant_ids = []
        for place_name, place_info in places_data.items():
            # Extract city from address
            city = extract_city_from_address(place_info['address'])
            
            # Convert rating to proper format
            rating = None
            if place_info.get('rating') not in (None, 'No rating'):
                try:
                    rating = float(place_info['rating'])
                except (ValueError, TypeError):
                    rating = None

            # Check if restaurant already exists
            existing_restaurant = db.query(Restaurant).filter(
                Restaurant.name == place_info['name'],
                Restaurant.location == place_info['address']
            ).first()
            
            if existing_restaurant:
                restaurant_ids.append(existing_restaurant.id)
                # Update existing restaurant info
                existing_restaurant.rating = rating
                existing_restaurant.price_level = {
                    'Free': 0, '$': 1, '$$': 2, '$$$': 3, '$$$$': 4
                }.get(place_info.get('price_level'))
                existing_restaurant.website = place_info.get('website')
                existing_restaurant.phone = place_info.get('phone')
                existing_restaurant.city = city
                existing_restaurant.updated_at = datetime.utcnow()
            else:
                # Create new restaurant
                new_restaurant = Restaurant(
                    name=place_info['name'],
                    location=place_info['address'],
                    location_link=place_info['google_maps_link'],
                    coordinates=f"{place_info['latitude']},{place_info['longitude']}",
                    rating=rating,
                    price_level={
                        'Free': 0, '$': 1, '$$': 2, '$$$': 3, '$$$$': 4
                    }.get(place_info.get('price_level')),
                    website=place_info.get('website'),
                    phone=place_info.get('phone'),
                    city=city
                )
                db.add(new_restaurant)
                db.flush()
                restaurant_ids.append(new_restaurant.id)

        # Now create video entries for each restaurant
        for restaurant_id in restaurant_ids:
            # Check if video already exists
            existing_video = db.query(Video).filter(
                Video.video_id == video_id,
                Video.platform == platform,
                Video.restaurant_id == restaurant_id
            ).first()
            
            if not existing_video:
                new_video = Video(
                    platform=platform,
                    video_id=video_id,
                    video_url=video_url,
                    creator_name=creator_info.get('creator_name'),
                    creator_id=creator_info.get('creator_id'),
                    view_count=creator_info.get('view_count'),
                    restaurant_id=restaurant_id
                )
                db.add(new_video)

        db.commit()
        print(f"Successfully updated database with {len(restaurant_ids)} restaurants and their associated videos")

    except SQLAlchemyError as e:
        db.rollback()
        print(f"Error updating database: {str(e)}")
        raise
    finally:
        db.close() 