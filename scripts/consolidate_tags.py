import sys
import os   
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import SessionLocal
from src.models.models import Tag, Restaurant, restaurant_tags
from sqlalchemy import func, and_
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define tag consolidation mappings
TAG_CONSOLIDATION = {
    # Cuisine Types
    'asian': ['asian', 'asian fusion', 'chinese', 'japanese', 'vietnamese', 'korean', 'taiwanese', 'malaysian', 'indonesian', 'dim sum', 'hot pot', 'franco-taiwanese fusion'],
    'european': ['european', 'european cuisine', 'modern european', 'contemporary european'],
    'mediterranean': ['mediterranean', 'mediterranean cuisine', 'spanish', 'spanish cuisine', 'greek', 'tapas'],
    'american': ['american', 'american cuisine', 'american bbq', 'bbq'],
    'burger': ['burger', 'burgers'],
    'indian': ['indian', 'indian_subcontinent', 'pakistani', 'nepalese'],
    'middle_eastern': ['middle eastern', 'middle eastern cuisine', 'middle_eastern', 'turkish', 'persian', 'lebanese', 'moroccan', 'algerian'],
    'latin_american': ['latin american', 'latin_american', 'brazilian', 'mexican', 'peruvian', 'colombian', 'caribbean'],
    
    # Dining Types
    'fine_dining': ['fine_dining', 'luxury', 'upscale', 'premium', 'michelin star', 'tasting menu'],
    'casual_dining': ['casual', 'casual dining', 'casual_dining'],
    'cafe': ['cafe', 'coffee shop', 'coffee specialty', 'dessert cafe', 'bakery', 'french bakery'],
    'bar': ['bar', 'wine bar', 'craft beer'],
    'quick_service': ['quick_service', 'fast food', 'fast casual', 'street food'],
    
    # Features
    'takeout': ['takeout', 'takeout available', 'delivery'],
    'dietary': ['vegetarian-friendly', 'gluten-free', 'gluten-free options', 'halal', 'health food', 'healthy'],
    'ambiance': ['historic', 'historic building', 'historic site', 'rooftop', 'waterfront', 'art-themed', 'instagrammable'],
    'special_meal': ['breakfast', 'brunch', 'dessert', 'desserts', 'buffet'],
    'market_dining': ['market', 'market dining', 'indoor market'],
}

def consolidate_tags():
    db = SessionLocal()
    try:
        # Get all existing tags
        existing_tags = {tag.name: tag for tag in db.query(Tag).all()}
        logger.info(f"Found {len(existing_tags)} existing tags")

        # Get set of main tags (the ones we want to keep)
        main_tags = set(TAG_CONSOLIDATION.keys())
        logger.info(f"Main consolidated tags: {main_tags}")

        # Process each consolidation
        for main_tag, subtags in TAG_CONSOLIDATION.items():
            logger.info(f"\nProcessing consolidation for {main_tag}")
            
            # Get or create the main tag
            if main_tag not in existing_tags:
                main_tag_obj = Tag(name=main_tag)
                db.add(main_tag_obj)
                db.flush()
                logger.info(f"Created new main tag: {main_tag}")
            else:
                main_tag_obj = existing_tags[main_tag]
                logger.info(f"Using existing main tag: {main_tag}")

            # Process each subtag
            for subtag in subtags:
                if subtag in existing_tags and subtag != main_tag:
                    old_tag = existing_tags[subtag]
                    
                    # Find restaurants with the old tag
                    restaurants = db.query(Restaurant)\
                        .join(restaurant_tags)\
                        .filter(restaurant_tags.c.tag_id == old_tag.id)\
                        .all()
                    
                    # Update restaurant associations
                    for restaurant in restaurants:
                        if not db.query(restaurant_tags).filter(
                            and_(
                                restaurant_tags.c.restaurant_id == restaurant.id,
                                restaurant_tags.c.tag_id == main_tag_obj.id
                            )
                        ).first():
                            restaurant.tags.append(main_tag_obj)
                            logger.info(f"Updated restaurant {restaurant.id} from {subtag} to {main_tag}")
                    
                    # Delete old tag association
                    db.query(restaurant_tags).filter(restaurant_tags.c.tag_id == old_tag.id).delete()
                    # Delete old tag
                    db.query(Tag).filter(Tag.id == old_tag.id).delete()
                    logger.info(f"Deleted old tag: {subtag}")

            db.commit()

        # Remove all tags that aren't main consolidated tags
        for tag_name, tag in existing_tags.items():
            if tag_name not in main_tags:
                # Move any remaining restaurants to appropriate consolidated tags
                restaurants = db.query(Restaurant)\
                    .join(restaurant_tags)\
                    .filter(restaurant_tags.c.tag_id == tag.id)\
                    .all()
                
                if restaurants:
                    logger.info(f"Removing unconsolidated tag: {tag_name} (used by {len(restaurants)} restaurants)")
                    # Delete tag associations
                    db.query(restaurant_tags).filter(restaurant_tags.c.tag_id == tag.id).delete()
                    # Delete tag
                    db.query(Tag).filter(Tag.id == tag.id).delete()
                else:
                    logger.info(f"Removing unused tag: {tag_name}")
                    db.query(Tag).filter(Tag.id == tag.id).delete()

        db.commit()
        logger.info("\nSuccessfully consolidated tags")
        
    except Exception as e:
        logger.error(f"Error in consolidate_tags: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

def print_tag_statistics():
    db = SessionLocal()
    try:
        tag_stats = db.query(Tag.name, func.count(Restaurant.id))\
            .select_from(Tag)\
            .outerjoin(restaurant_tags)\
            .outerjoin(Restaurant)\
            .group_by(Tag.name)\
            .all()
        
        logger.info("\nTag usage statistics:")
        for tag_name, count in sorted(tag_stats, key=lambda x: x[1], reverse=True):
            logger.info(f"{tag_name}: {count} restaurants")
            
    finally:
        db.close()

def print_unconsolidated_tags():
    db = SessionLocal()
    try:
        # Get all existing tags
        existing_tags = {tag.name for tag in db.query(Tag).all()}
        
        # Get all tags that are part of consolidation
        consolidated_tags = set()
        for main_tag, subtags in TAG_CONSOLIDATION.items():
            consolidated_tags.add(main_tag)
            consolidated_tags.update(subtags)
        
        # Find tags that aren't part of consolidation
        unconsolidated = existing_tags - consolidated_tags
        
        logger.info("\nUnconsolidated tags:")
        for tag in sorted(unconsolidated):
            logger.info(f"- {tag}")
            
    finally:
        db.close()

if __name__ == "__main__":
    consolidate_tags()
    print_tag_statistics()
    print_unconsolidated_tags()