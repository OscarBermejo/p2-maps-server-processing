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
        f"#{restaurant_name}",
        f"#{restaurant_name}{city}",
        f"#{city}",
        f"#{restaurant_name}restaurant"
    ]
    
    return hashtags

def search_tiktok_videos(restaurant_name, city, max_videos=5):
    """
    Search TikTok for videos about a specific restaurant using hashtags
    Returns list of videos sorted by views
    """
    # Keywords that indicate the video is about a restaurant/food
    restaurant_keywords = {
        # Restaurant-related
        'restaurant', 'restaurante', 'dining', 'dinner', 'lunch', 'brunch',
        'michelin', 'chef', 'cuisine', 'eatery', 'bistro',
        
        # Food-related
        'food', 'foodie', 'meal', 'dish', 'menu', 'plate', 'tasting',
        'delicious', 'yummy', 'tasty', 'gastronomy', 'gastronomia',
        
        # Specific meal types
        'appetizer', 'starter', 'main course', 'dessert', 'entrée',
        
        # Local variations (add based on your location)
        'comida', 'restauracja', 'ristorante'  # Spanish, Polish, Italian
    }

    try:
        hashtags = create_search_hashtags(restaurant_name, city)
        logger.info(f"Searching TikTok with hashtags: {', '.join(hashtags)}")
        
        videos = []
        with TikTokAPI() as api:
            for hashtag in hashtags:
                try:
                    tag_name = hashtag.replace('#', '')
                    challenge = api.challenge(tag_name)
                    if challenge:
                        for video in challenge.videos:
                            try:
                                # Get video description and hashtags
                                description = getattr(video, 'desc', '').lower() or ''
                                video_hashtags = getattr(video, 'hashtags', []) or []
                                video_hashtag_names = {tag.name.lower() for tag in video_hashtags}
                                
                                # Verify that the searched hashtag is actually in the video
                                searched_tag = hashtag.replace('#', '').lower()
                                if searched_tag not in video_hashtag_names and hashtag not in description.lower():
                                    logger.info(f"Skipping video - doesn't contain searched hashtag {hashtag}")
                                    continue
                                
                                # Check if any of the keywords are in the description
                                if not any(keyword in description.lower() for keyword in restaurant_keywords):
                                    logger.info(f"Skipping video - no relevant keywords in description: {description[:100]}...")
                                    continue
                                
                                # Get other video details
                                author_username = getattr(video.author, 'unique_id', None) or getattr(video.author, 'username', 'unknown')
                                stats = getattr(video, 'stats', None)
                                play_count = getattr(stats, 'play_count', 0) if stats else 0
                                digg_count = getattr(stats, 'digg_count', 0) if stats else 0
                                
                                video_info = {
                                    'url': f"https://www.tiktok.com/@{author_username}/video/{video.id}",
                                    'views': play_count,
                                    'likes': digg_count,
                                    'creator': author_username,
                                    'hashtag': hashtag,
                                    'description': description,
                                    'video_hashtags': list(video_hashtag_names),
                                    'matched_keywords': [word for word in restaurant_keywords if word in description.lower()]
                                }
                                videos.append(video_info)
                                logger.info(f"Added video with hashtag {hashtag} and keywords: {video_info['matched_keywords']}")
                                
                                if len(videos) >= max_videos:
                                    break
                                    
                            except AttributeError as e:
                                logger.warning(f"Skipping video - missing attribute: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Error processing video for hashtag {hashtag}: {e}")
                                continue
                                
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing hashtag {hashtag}: {e}")
                    continue
                
            # Sort videos by views
            videos.sort(key=lambda x: x['views'], reverse=True)
            
            # Print which keywords matched for each video
            for video in videos[:max_videos]:
                logger.info(f"Selected video matched keywords: {video['matched_keywords']}")
                logger.info(f"Video hashtags: {video['video_hashtags']}")
            
            return videos[:max_videos]
            
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
        
        # Process only "Imprevisto" restaurant
        for restaurant in restaurants:
            if restaurant.get('name') != 'Imprevisto':
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
                    print(f"   Description: {video['description']}")
                    print(f"   Video hashtags: {', '.join(video['video_hashtags'])}")
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