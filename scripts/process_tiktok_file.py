import json
import os
from tiktokapipy.api import TikTokAPI
import time
import logging
import warnings

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
        f"#{restaurant_name}{city}",
        f"#{restaurant_name}restaurant"
    ]
    
    return hashtags

def search_tiktok_videos(restaurant_name, city, max_videos=5):
    """
    Search TikTok for videos about a specific restaurant using hashtags
    Returns list of videos sorted by views
    """
    try:
        hashtags = create_search_hashtags(restaurant_name, city)
        logger.info(f"Searching TikTok with hashtags: {', '.join(hashtags)}")
        
        videos = []
        with TikTokAPI() as api:
            for hashtag in hashtags:
                try:
                    # Remove the # symbol for the search
                    tag_name = hashtag.replace('#', '')
                    
                    # Use challenge_info and challenge_videos methods
                    challenge = api.challenge(tag_name)
                    if challenge:
                        for video in challenge.videos:
                            try:
                                # Get author info safely
                                author_username = getattr(video.author, 'unique_id', None) or getattr(video.author, 'username', 'unknown')
                                
                                # Get video stats safely
                                stats = getattr(video, 'stats', None)
                                play_count = getattr(stats, 'play_count', 0) if stats else 0
                                digg_count = getattr(stats, 'digg_count', 0) if stats else 0
                                
                                video_info = {
                                    'url': f"https://www.tiktok.com/@{author_username}/video/{video.id}",
                                    'views': play_count,
                                    'likes': digg_count,
                                    'creator': author_username,
                                    'hashtag': hashtag
                                }
                                videos.append(video_info)
                                logger.info(f"Successfully processed video: {video.id}")
                                
                                if len(videos) >= max_videos:
                                    break
                                    
                            except AttributeError as e:
                                logger.warning(f"Skipping video - missing attribute: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Error processing video for hashtag {hashtag}: {e}")
                                continue
                                
                    # Be nice to TikTok's servers
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing hashtag {hashtag}: {e}")
                    continue
                
            # Sort videos by views
            videos.sort(key=lambda x: x['views'], reverse=True)
            
            return videos[:max_videos]  # Return only max_videos number of videos
            
    except Exception as e:
        logger.error(f"Error searching TikTok for {restaurant_name}: {e}")
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
        
        # Process each restaurant
        for restaurant in restaurants:
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
            else:
                print("No TikTok videos found")
                
            print("\n--------------------------------")

        print(f"\nTotal restaurants processed: {len(restaurants)}")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON file: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    # File path to michelin_restaurants.json
    file_path = "/home/ec2-user/maps-server-processing/scripts/web_processing/michelin_restaurants.json"
    
    # Process the file
    process_michelin_file(file_path)