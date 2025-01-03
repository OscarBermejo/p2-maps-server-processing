import json
import os
from tiktokapipy.api import TikTokAPI
import time
import logging
import warnings
import yt_dlp

# Silence all warnings from tiktokapipy
warnings.filterwarnings('ignore', module='tiktokapipy')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    # Keywords that indicate the video is about a restaurant
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
                logger.info(f"Searching TikTok with hashtag: {hashtag}")
                
                try:
                    clean_tag = hashtag.replace('#', '')
                    logger.info(f"Fetching challenge for tag: {clean_tag}")
                    
                    challenge = api.challenge(clean_tag)
                    logger.info("Challenge fetched successfully")
                    
                    video_count = 0
                    start_time = time.time()
                    timeout = 30
                    
                    for video in challenge.videos:
                        if time.time() - start_time > timeout:
                            logger.warning(f"Timeout reached for hashtag {hashtag}")
                            break
                            
                        video_count += 1
                        logger.info(f"Processing video {video_count} for {hashtag}")
                        
                        try:
                            description = video.desc.lower()
                            logger.info(f"Video description: {description[:100]}...")
                            
                            # More strict filtering:
                            # 1. Must contain at least one restaurant keyword
                            matched_keywords = [word for word in restaurant_keywords if word in description]
                            if not matched_keywords:
                                logger.info("Skipping video - no restaurant keywords found")
                                continue
                                
                            # 2. Must contain either city name or restaurant name
                            if not (city.lower() in description or restaurant_name.lower() in description):
                                logger.info("Skipping video - neither city nor restaurant name found")
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
                            logger.info(f"Added video from hashtag {hashtag} with keywords: {matched_keywords}")
                            
                        except Exception as e:
                            logger.error(f"Error processing video: {str(e)}")
                            continue
                            
                        if video_count >= 20:
                            logger.info(f"Reached maximum video count for hashtag {hashtag}")
                            break
                            
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing hashtag {hashtag}: {str(e)}")
                    continue
        
        # Remove duplicates based on URL
        unique_videos = {v['url']: v for v in all_videos}.values()
        
        # Sort all collected videos by views and return top max_videos
        sorted_videos = sorted(unique_videos, key=lambda x: x['views'], reverse=True)
        return sorted_videos[:max_videos]
            
    except Exception as e:
        logger.error(f"Error searching TikTok for {restaurant_name}: {str(e)}")
        return []

def process_michelin_file(file_path):
    """
    Process Michelin restaurants JSON file and search for TikTok videos
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"Error: File not found at {file_path}")
            return

        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            restaurants = json.load(f)

        print("\nMichelin Restaurants and TikTok Videos:")
        print("=====================================")
        
        # Process only "Imprevisto" restaurant
        for restaurant in restaurants:
            if restaurant.get('name') != 'MAE Barcelona':
                continue
                
            name = restaurant.get('name', 'N/A')
            city = restaurant.get('city', 'N/A')
            
            print(f"\nRestaurant: {name}")
            print(f"City: {city}")
            print(f"Stars: {restaurant.get('stars', 'N/A')}")
            print(f"Location: {restaurant.get('location', 'N/A')}")
            print(f"Telephone: {restaurant.get('telephone', 'N/A')}")
            print(f"Cuisine: {restaurant.get('cuisine', 'N/A')}")
            
            print("\nTop TikTok Videos:")
            videos = search_tiktok_videos(name, city)
            
            if videos:
                for i, video in enumerate(videos, 1):
                    print(f"\n{i}. Video URL: {video['url']}")
                    print(f"   Views: {video['views']:,}")
                    print(f"   Likes: {video['likes']:,}")
                    print(f"   Creator: {video['creator']}")
                    print(f"   Found via: {video['hashtag']}")
                    print(f"   Description: {video['description'][:200]}...")
                    print(f"   Matched keywords: {', '.join(video['matched_keywords'])}")
                    
            else:
                print("No TikTok videos found")
                
            print("\n--------------------------------")
            break  # Exit after processing Imprevisto

        print(f"\nProcessing complete for Imprevisto restaurant")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON file: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    # File path to michelin_restaurants.json
    file_path = "/home/ec2-user/maps-server-processing/scripts/web_processing/michelin_restaurants.json"
    
    # Process the file
    process_michelin_file(file_path)