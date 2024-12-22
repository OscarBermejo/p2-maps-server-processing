from tiktokapipy.api import TikTokAPI
from typing import List
import time
from sqlalchemy import select
import sys
import os
import logging

logger = logging.getLogger(__name__)

# Suppress SQLAlchemy logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
# Suppress MySQL Connector logging
logging.getLogger('mysql.connector').setLevel(logging.WARNING)

# Replace the existing sys.path.append line with these:
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from src.tasks.video_tasks import process_video
from src.database import SessionLocal
from src.models.models import Video, ProcessedVideo

# Barcelona-specific restaurant hashtags
BARCELONA_HASHTAGS = [
    "foodbarcelona",
    "barcelonafoodie",
    "comerenbcn",
    "comerbarcelona",
    "barcelonaeats",
    "dondecomerbcn",
    "barcelonafoodguide",
    "bcnfood",
    "bcnfoodie",
    "barcelonafoodies",
    "restaurantsbarcelona",
    "barcelonagastronomia"
]

# Melbourne-specific restaurant hashtags
MELBOURNE_HASHTAGS = [
    "melbournerestaurants",
    "melbournefood",
    "melbournefoodie",
    "melbourneeats",
    "foodmelbourne",
    "melbournefoodscene",
    "melbournefoodguide",
    "melbournefoodspots",
    "melbournefoodblog",
    "melbournecafe",
    "wheretoeatingmelbourne",
    "melbournebrunch",
    "melbournedining",
    "visitmelbourne",
    "melbournefoodshare"
]

# Antwerp-specific restaurant hashtags
ANTWERP_HASHTAGS = [
    "antwerpfoodie",
    "restaurantsantwerpen",
    "antwerpenrestaurants",
    "eteninantwerpen",
    "antwerpfoodguide",
    "antwerprestaurants",
    "foodantwerp",
    "antwerpfoodspots",
    "antwerpfoodscene",
    "wheretoeatingantwerp",
    "visitantwerp",
    "antwerpbrunch",
    "antwerpdining"
]

# Add Amsterdam-specific restaurant hashtags
AMSTERDAM_HASHTAGS = [
    "amsterdamfood",
    "amsterdamfoodie",
    "eetenamsterdam",
    "amsterdameats",
    "restaurantsamsterdam",
    "amsterdamrestaurants",
    "foodamsterdam",
    "wheretoeatinamsterdam",
    "amsterdamfoodguide",
    "amsterdamfoodspots",
    "amsterdamfoodscene",
    "etendrinkeninamsterdam",
    "bestfoodamsterdam",
    "amsterdambrunch",
    "dineamsterdam"
]

def video_exists(video_id: str, db_session) -> bool:
    """Check if video has been processed before"""
    logger.debug(f"Checking if video {video_id} exists in processed_videos")
    query = select(ProcessedVideo).where(ProcessedVideo.video_id == video_id)
    result = db_session.execute(query).first()
    logger.debug(f"Video exists in processed_videos: {result is not None}")
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

def get_challenge_videos(hashtag: str, max_videos: int = 10) -> List[dict]:
    logger.info(f"Starting get_challenge_videos for #{hashtag}")
    video_data = []
    
    logger.info("Initializing TikTokAPI")
    with TikTokAPI(
        headless=True,
        navigation_timeout=60000
    ) as api:
        try:
            hashtag = hashtag.replace('#', '')
            logger.debug(f"Cleaned hashtag: #{hashtag}")
            
            logger.info("Fetching challenge data from TikTok")
            challenge = api.challenge(hashtag, video_limit=max_videos)
            
            logger.info("Opening database session")
            db = SessionLocal()
            
            logger.info("Starting to process videos")
            for video in challenge.videos:
                video_id = str(video.id)
                logger.debug(f"Processing video ID: {video_id}")
                
                if video_exists(video_id, db):
                    logger.debug(f"Video {video_id} already exists, skipping")
                    continue
                
                try:
                    video_info = {
                        'url': f"https://www.tiktok.com/@{video.author.unique_id}/video/{video_id}",
                        'views': getattr(video.stats, 'play_count', 0),
                        'likes': getattr(video.stats, 'digg_count', 0),
                        'video_id': video_id
                    }
                    video_data.append(video_info)
                except AttributeError as e:
                    logger.error(f"Skipping video due to missing attributes: {e}")
                    continue
            
            db.close()
            
            # Sort videos by view count (descending)
            video_data.sort(key=lambda x: x['views'], reverse=True)
            return video_data[:max_videos]
                
        except Exception as e:
            logger.error(f"Error fetching challenge videos: {e}", exc_info=True)
            logger.error(f"Error type: {type(e)}")
            return []

def process_hashtag_videos(hashtag: str, max_videos: int = 100):
    logger.info(f"=== Fetching TikTok videos for hashtag: #{hashtag} ===")
    videos = get_challenge_videos(hashtag, max_videos)
    logger.info(f"Found {len(videos)} new videos to process")
    
    db = SessionLocal()
    
    for i, video in enumerate(videos, 1):
        try:
            video_id = video['video_id']
            video_url = video['url']
            
            # Skip if video was already processed
            if video_exists(video_id, db):
                logger.info(f"Skipping video {video_id} - already processed")
                continue
                
            logger.info(f"Processing video {i}/{len(videos)}: {video_url}")
            logger.info(f"Views: {video.get('views', 'N/A')}")
            
            try:
                # Process the video
                process_video(video_url)
                
                # Check if restaurants were found (video exists in the Video table)
                has_restaurants = db.execute(
                    select(Video).where(Video.video_id == video_id)
                ).first() is not None
                
                # Mark video as processed
                mark_video_as_processed(video_id, video_url, has_restaurants, db)
                
            except Exception as e:
                # If processing fails, still mark it as processed but with no restaurants
                logger.error(f"Failed to process video {video_url}: {str(e)}")
                mark_video_as_processed(video_id, video_url, False, db)
                continue
                
            # Add longer sleep between videos to avoid rate limiting
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error processing video entry: {str(e)}")
            continue
    
    db.close()

if __name__ == "__main__":
    total_processed = 0
    failed_hashtags = []
    
    logger.info(f"Starting to process {len(AMSTERDAM_HASHTAGS)} hashtags for Amsterdam restaurants...")
    
    for hashtag in AMSTERDAM_HASHTAGS:
        try:
            logger.info("="*50)
            logger.info(f"Processing hashtag: {hashtag}")
            logger.info("="*50)
            process_hashtag_videos(hashtag, max_videos=100)
            total_processed += 1
        except Exception as e:
            logger.error(f"Failed to process hashtag #{hashtag}: {str(e)}", exc_info=True)
            failed_hashtags.append(hashtag)
            continue
            
    logger.info("="*50)
    logger.info("Processing completed!")
    logger.info(f"Successfully processed {total_processed}/{len(AMSTERDAM_HASHTAGS)} hashtags")
    if failed_hashtags:
        logger.warning(f"Failed hashtags: {', '.join(failed_hashtags)}")