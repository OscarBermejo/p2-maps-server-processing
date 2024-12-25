import yt_dlp
import logging
from sqlalchemy import select, text
import time
import sys
import os
import psutil
import datetime
import glob

# Setup logging
logger = logging.getLogger(__name__)

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from src.tasks.video_tasks import process_video
from src.database import SessionLocal
from src.models.models import Video, ProcessedVideo, Tag, Restaurant, restaurant_tags

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
        logger.info(f"Marked video {video_id} as processed (has_restaurants={has_restaurants})")
    except Exception as e:
        logger.error(f"Error marking video as processed: {e}")
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
        
        logger.info(f"""
System Resources:
----------------
Time: {datetime.datetime.now()}
CPU Usage: {cpu_percent}%
Memory: {memory_used_gb:.2f}GB / {memory_total_gb:.2f}GB ({memory_percent}%)
Disk: {disk_used_gb:.2f}GB / {disk_total_gb:.2f}GB ({disk.percent}%)
        """)
    except Exception as e:
        logger.error(f"Error logging system resources: {e}")

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
                logger.info(f"Removed audio file: {file}")
            except Exception as e:
                logger.error(f"Error removing audio file {file}: {e}")
        
        # Remove video files
        video_files = glob.glob(video_path)
        for file in video_files:
            try:
                os.remove(file)
                logger.info(f"Removed video file: {file}")
            except Exception as e:
                logger.error(f"Error removing video file {file}: {e}")
                
        logger.info(f"Cleanup complete. Removed {len(audio_files)} audio files and {len(video_files)} video files")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def get_tiktok_videos(username):
    logger.info(f"Starting to process videos for TikTok user: {username}")
    db = SessionLocal()
    
    try:
        # Log initial resource usage
        log_system_resources()
        
        # Configure yt-dlp options
        ydl_opts = {
            'quiet': True,
            'extract_flat': True,
            'force_generic_extractor': False
        }
        
        url = f'https://www.tiktok.com/@{username}'
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
            if 'entries' in info:
                for entry in info['entries']:
                    # Log resources before processing each video
                    log_system_resources()
                    
                    video_id = entry['id']
                    video_url = entry['url']
                    
                    if video_exists(video_id, db):
                        logger.info(f"Skipping video {video_id} - already processed")
                        continue
                    
                    try:
                        print("1. Starting to process video...")
                        # Process the video
                        logger.info(f"Processing video: {video_url}")
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
                            logger.info(f"Restaurant found for video {video_id}, restaurant_id: {raw_result.restaurant_id}")
                            add_curated_tag_to_restaurant(db, raw_result.restaurant_id)
                            logger.info(f"Added curated tag to restaurant {raw_result.restaurant_id}")
                        else:
                            print("5. No restaurant found")
                            logger.info(f"No restaurant found for video {video_id}")

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
                        
                    except Exception as e:
                        logger.error(f"Failed to process video {video_url}: {str(e)}")
                        # Still try to clean up even if processing failed
                        cleanup_files()
                        continue
                    
                    # Add garbage collection after each video
                    import gc
                    gc.collect()
                    
                    time.sleep(5)
            
        logger.info(f"Finished processing videos for user: {username}")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Final cleanup
        cleanup_files()
        db.close()

if __name__ == "__main__":
    try:
        # First, check if we have enough resources to start
        log_system_resources()
        
        username = input("Enter TikTok username: ")
        get_tiktok_videos(username)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Try to clean up even if there's a fatal error
        cleanup_files()