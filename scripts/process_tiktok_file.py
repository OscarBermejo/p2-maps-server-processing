import json
import os
from tiktokapipy.api import TikTokAPI
import time
import logging
import warnings
import sys
import os
from sqlalchemy import select, text
import psutil
import datetime
# Add garbage collection after each video
import gc

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from src.utils.logger_config import setup_cloudwatch_logging

# Silence all warnings from tiktokapipy
warnings.filterwarnings('ignore', module='tiktokapipy')

# Setup logging with both CloudWatch and terminal output
logger = setup_cloudwatch_logging(app_name='maps-server')

# Add StreamHandler for terminal output
stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(message)s')  # Simplified format for terminal
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)

# Test log to verify configuration
print("Logging system initialized")

from src.tasks.video_tasks import process_video
from src.database import SessionLocal
from src.models.models import Video, ProcessedVideo, Tag, Restaurant, restaurant_tags

def ensure_tag(db_session, tag_name):
    """Ensure tag exists in database"""
    try:
        tag = db_session.query(Tag).filter(Tag.name == tag_name).first()
        if not tag:
            logger.info(f"Creating new '{tag_name}' tag")
            tag = Tag(name=tag_name)
            db_session.add(tag)
            db_session.commit()
            logger.info(f"Successfully created '{tag_name}' tag")
        return tag
    except Exception as e:
        logger.error(f"Error ensuring {tag_name} tag: {e}")
        db_session.rollback()
        raise

def add_tags_to_restaurant(db_session, restaurant_id):
    """Add curated and michelin tags to a restaurant"""
    try:
        # Get or create both tags
        curated_tag = ensure_tag(db_session, "curated")
        michelin_tag = ensure_tag(db_session, "michelin")
        
        # Get restaurant
        restaurant = db_session.query(Restaurant).filter(Restaurant.id == restaurant_id).first()
        
        if restaurant:
            # Add curated tag if not present
            if curated_tag not in restaurant.tags:
                logger.info(f"Adding curated tag to restaurant: {restaurant.name}")
                restaurant.tags.append(curated_tag)
            
            # Add michelin tag if not present
            if michelin_tag not in restaurant.tags:
                logger.info(f"Adding michelin tag to restaurant: {restaurant.name}")
                restaurant.tags.append(michelin_tag)
            
            db_session.commit()
            logger.info(f"Successfully added tags to restaurant: {restaurant.name}")
        else:
            logger.error(f"Restaurant {restaurant_id} not found")
            
    except Exception as e:
        logger.error(f"Error adding tags to restaurant {restaurant_id}: {e}")
        db_session.rollback()
        raise

def video_exists(video_id: str, db_session) -> bool:
    """Check if video has been processed before"""
    query = select(ProcessedVideo).where(ProcessedVideo.video_id == video_id)
    result = db_session.execute(query).first()
    return result is not None

def mark_video_as_processed(video_id: str, url: str, has_restaurants: bool, db_session):
    """Mark a video as processed in the database"""
    try:
        processed_video = ProcessedVideo(
            video_id=video_id,
            platform='tiktok',
            has_restaurants=has_restaurants,
            video_url=url
        )
        db_session.add(processed_video)
        db_session.commit()
        print(f"Marked video {video_id} as processed (has_restaurants={has_restaurants})")
    except Exception as e:
        print(f"Error marking video as processed: {e}")
        db_session.rollback()

def log_system_resources():
    """Log current system resource usage"""
    try:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Memory usage
        memory = psutil.virtual_memory()
        memory_used_gb = memory.used / (1024 * 1024 * 1024)  # Convert to GB
        memory_total_gb = memory.total / (1024 * 1024 * 1024)  # Convert to GB
        memory_percent = memory.percent
        
        # Disk usage
        disk = psutil.disk_usage('/')
        disk_used_gb = disk.used / (1024 * 1024 * 1024)  # Convert to GB
        disk_total_gb = disk.total / (1024 * 1024 * 1024)  # Convert to GB
        
        print(f"""
            System Resources:
            ----------------
            Time: {datetime.datetime.now()}
            CPU Usage: {cpu_percent}%
            Memory: {memory_used_gb:.2f}GB / {memory_total_gb:.2f}GB ({memory_percent}%)
            Disk: {disk_used_gb:.2f}GB / {disk_total_gb:.2f}GB ({disk.percent}%)
                    """)
    except Exception as e:
        print(f"Error logging system resources: {e}")

def cleanup_files():
    """Clean up audio and video files"""
    try:
        # Paths to clean
        audio_path = "/home/ec2-user/maps-server-processing/files/audio/*.wav"
        video_path = "/home/ec2-user/maps-server-processing/files/video/*.mp4"
        
        # Remove audio files
        audio_files = glob.glob(audio_path)
        for file in audio_files:
            try:
                os.remove(file)
                print(f"Removed audio file: {file}")
            except Exception as e:
                print(f"Error removing audio file {file}: {e}")
        
        # Remove video files
        video_files = glob.glob(video_path)
        for file in video_files:
            try:
                os.remove(file)
                print(f"Removed video file: {file}")
            except Exception as e:
                print(f"Error removing video file {file}: {e}")
                
        print(f"Cleanup complete. Removed {len(audio_files)} audio files and {len(video_files)} video files")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")

def create_search_hashtags(restaurant_name, city):
    """
    Create search hashtags from restaurant name and city
    Remove spaces and special characters
    """
    # Clean restaurant name and city
    restaurant_name = ''.join(e for e in restaurant_name if e.isalnum()).lower()
    city = city.lower().replace(' ', '')
    
    # Create hashtag combinations - only restaurant specific
    hashtags = [
        f"#{restaurant_name}",
        f"#{restaurant_name}{city}",
        f"#{restaurant_name}restaurant"
    ]
    
    return hashtags

def search_tiktok_videos(restaurant_name, city, max_videos=5):
    """
    Search TikTok for videos about a specific restaurant using multiple hashtags
    Returns list of videos sorted by views
    """
    restaurant_keywords = {
        'restaurant', 'food', 'dining', 'meal', 'michelin',
        'chef', 'cuisine', 'foodie', 'menu', 'eating',
        'restaurante', 'comida', 'cocina'  # Spanish variations
    }

    try:
        search_hashtags = create_search_hashtags(restaurant_name, city)
        all_videos = []
        
        with TikTokAPI() as api:
            for hashtag in search_hashtags:
                print(f"Searching TikTok with hashtag: {hashtag}")
                
                try:
                    clean_tag = hashtag.replace('#', '')
                    print(f"Fetching challenge for tag: {clean_tag}")
                    
                    challenge = api.challenge(clean_tag)
                    print("Challenge fetched successfully")
                    
                    for video in challenge.videos:
                        try:
                            description = video.desc.lower()
                            video_hashtags = [tag.lower() for tag in video.hashtags] if hasattr(video, 'hashtags') else []
                            
                            # 1. Must contain at least one restaurant keyword
                            matched_keywords = [word for word in restaurant_keywords if word in description]
                            if not matched_keywords:
                                print("Skipping video - no restaurant keywords found")
                                continue
                                
                            # 2. Must contain city name in either description or hashtags
                            city_lower = city.lower()
                            city_in_description = city_lower in description
                            city_in_hashtags = any(city_lower in tag for tag in video_hashtags)
                            
                            if not (city_in_description or city_in_hashtags):
                                logger.info(f"Skipping video - city '{city}' not found in description or hashtags")
                                continue
                            
                            video_info = {
                                'url': f"https://www.tiktok.com/@{video.author.unique_id}/video/{video.id}",
                                'views': video.stats.play_count,
                                'likes': video.stats.digg_count,
                                'creator': video.author.unique_id,
                                'description': video.desc,
                                'matched_keywords': matched_keywords,
                                'hashtag': hashtag
                            }
                            all_videos.append(video_info)
                            print(f"Added video from hashtag {hashtag} with keywords: {matched_keywords}")
                            
                        except Exception as e:
                            print(f"Error processing video: {str(e)}")
                            continue
                            
                        if video_count >= 20:
                            print(f"Reached maximum video count for hashtag {hashtag}")
                            break
                            
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"Error processing hashtag {hashtag}: {str(e)}")
                    continue
        
        # Remove duplicates based on URL
        unique_videos = {v['url']: v for v in all_videos}.values()
        
        # Sort all collected videos by views and return top max_videos
        sorted_videos = sorted(unique_videos, key=lambda x: x['views'], reverse=True)
        return sorted_videos[:max_videos]
            
    except Exception as e:
        print(f"Error searching TikTok for {restaurant_name}: {str(e)}")
        return []
    
def process_video_url(video_url, video_id, db):
    """
    Process a TikTok video
    """
    print("1. Starting to process video...")
    # Process the video
    print(f"Processing video: {video_url}")
    process_video(video_url)
    
    print("2. Video processed, committing changes...")
    db.commit()
    
    print("3. About to execute raw SQL query...")
    raw_result = db.execute(
        text("""
            SELECT id, video_id, restaurant_id FROM videos WHERE video_id = :vid
        """), 
        {'vid': str(video_id)}
    ).first()
    print('4. Raw SQL result: ', raw_result)
    
    has_restaurants = False
    if raw_result and raw_result.restaurant_id:
        print("5. Found restaurant_id: ", raw_result.restaurant_id)
        has_restaurants = True
        print(f"Restaurant found for video {video_id}, restaurant_id: {raw_result.restaurant_id}")
        add_tags_to_restaurant(db, raw_result.restaurant_id)
        print(f"Added tags to restaurant {raw_result.restaurant_id}")
    else:
        print("5. No restaurant found")
        print(f"No restaurant found for video {video_id}")

    print('6. has_restaurants: ', has_restaurants)
    print('7. raw_result: ', raw_result)
    
    mark_video_as_processed(video_id, video_url, has_restaurants, db)
    
    # Log resources after processing each video
    log_system_resources()
    
    # Clean up files after processing each video
    cleanup_files()
    
    # Force garbage collection
    gc.collect()
    
    time.sleep(5)

def process_michelin_file(file_path):
    """
    Process Michelin restaurants JSON file and search for TikTok videos
    """
    global logger  # Use the global logger instance
    db = SessionLocal()
    
    try:
        # Test log
        print("Starting process_michelin_file function")
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return

        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            restaurants = json.load(f)
            print(f"Successfully loaded JSON file with {len(restaurants)} restaurants")

        print("\nMichelin Restaurants and TikTok Videos:")
        print("=====================================")
        
        for restaurant in restaurants:
            #if restaurant.get('name') != 'Capet':
            #   continue
                
            name = restaurant.get('name', 'N/A')
            city = restaurant.get('city', 'N/A')
            
            print(f"\nRestaurant: {name}")
            print(f"City: {city}")
            print(f"Stars: {restaurant.get('stars', 'N/A')}")
            print(f"Location: {restaurant.get('location', 'N/A')}")
            print(f"Telephone: {restaurant.get('telephone', 'N/A')}")
            print(f"Cuisine: {restaurant.get('cuisine', 'N/A')}")
            
            print("\nSearching for TikTok Videos...")
            videos = search_tiktok_videos(name, city)
            
            if videos:
                for i, video in enumerate(videos, 1):

                    video_url = video['url']
                    video_id = video_url.split('/')[-1]

                    print(f"\n{i}. Video URL: {video_url}")
                    print(f"   Video ID: {video_id}")
                    print(f"   Views: {video['views']:,}")
                    print(f"   Likes: {video['likes']:,}")
                    print(f"   Creator: {video['creator']}")
                    print(f"   Found via: {video['hashtag']}")
                    print(f"   Description: {video['description'][:200]}...")
                    print(f"   Matched keywords: {', '.join(video['matched_keywords'])}")

                    try:
                        if video_exists(video_id, db):
                            logger.info(f"Skipping video {video_id} - already processed")
                            continue

                        process_video_url(video_url, video_id, db)

                    except Exception as e:
                        print(f"Error processing video {video_url}: {str(e)}")

                #break
                    
            else:
                print("No TikTok videos found")
                
            print("\n--------------------------------")

        print(f"\nProcessing complete for {name} restaurant")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # File path to michelin_restaurants.json
    file_path = "/home/ec2-user/maps-server-processing/scripts/web_processing/michelin_restaurants.json"
    
    print("Starting script execution")
    # Process the file
    process_michelin_file(file_path)
    print("Script execution completed")