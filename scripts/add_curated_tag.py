import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import SessionLocal
from src.models.models import Restaurant, Tag, ProcessedVideo, Video
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_curated_tag(db_session):
    """Ensure 'curated' tag exists in database"""
    try:
        curated_tag = db_session.query(Tag).filter(Tag.name == "curated").first()
        if not curated_tag:
            logger.info("Creating new 'curated' tag")
            curated_tag = Tag(name="curated")
            db_session.add(curated_tag)
            db_session.commit()
            logger.info("Successfully created 'curated' tag")
        return curated_tag
    except Exception as e:
        logger.error(f"Error ensuring curated tag: {e}")
        db_session.rollback()
        raise

def add_curated_tag_to_restaurant(db_session, restaurant_id):
    """Add curated tag to a restaurant"""
    try:
        # Get or create curated tag
        curated_tag = ensure_curated_tag(db_session)
        
        # Get restaurant
        restaurant = db_session.query(Restaurant).filter(Restaurant.id == restaurant_id).first()
        
        if restaurant and curated_tag:
            # Check if tag is already assigned
            if curated_tag not in restaurant.tags:
                logger.info(f"Adding curated tag to restaurant: {restaurant.name}")
                restaurant.tags.append(curated_tag)
                db_session.commit()
                logger.info(f"Successfully added curated tag to restaurant: {restaurant.name}")
            else:
                logger.info(f"Restaurant {restaurant.name} already has curated tag")
    except Exception as e:
        logger.error(f"Error adding curated tag to restaurant {restaurant_id}: {e}")
        db_session.rollback()
        raise

def add_curated_tag():
    db = SessionLocal()
    try:
        target_date = datetime(2024, 12, 30, 20, 0, 0)
        
        # Find restaurants with videos processed after the specified date
        restaurants = (db.query(Restaurant)
            .join(Restaurant.videos)
            .join(ProcessedVideo, ProcessedVideo.video_id == Video.video_id)
            .filter(ProcessedVideo.processed_at > target_date)
            .distinct()
            .all())

        count = 0
        for restaurant in restaurants:
            add_curated_tag_to_restaurant(db, restaurant.id)
            count += 1

        logger.info(f"Successfully processed {count} restaurants")

    except Exception as e:
        logger.error(f"Error in add_curated_tag: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    add_curated_tag()