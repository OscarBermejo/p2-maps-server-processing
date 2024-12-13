from typing import Optional, Dict
import asyncio
from TikTokApi import TikTokApi  # Official TikTok API package
import os
from dotenv import load_dotenv
import traceback

class TikTokLocationExtractor:
    def __init__(self):
        load_dotenv()
        # Initialize TikTok API with ms_token from environment variables
        self.api = TikTokApi()

    def extract_video_id(self, url: str) -> str:
        """Extract video ID from TikTok URL."""
        try:
            # Example URL: https://www.tiktok.com/@username/video/1234567890
            video_id = url.split('video/')[1].split('?')[0]
            return video_id
        except Exception as e:
            print(f"Error extracting video ID: {e}")
            return None

    async def get_location_info(self, video_id: str) -> Optional[Dict]:
        """Get location information from a TikTok video."""
        try:
            # Initialize TikTokApi without additional setup
            api = TikTokApi()
            
            video = api.video(id=video_id)
            video_info = await video.info()

            # Extract location data if available
            if 'location' in video_info:
                location = {
                    'poi_name': video_info['location'].get('poi_name'),
                    'address': video_info['location'].get('address'),
                    'latitude': video_info['location'].get('latitude'),
                    'longitude': video_info['location'].get('longitude'),
                    'city': video_info['location'].get('city'),
                    'country': video_info['location'].get('country')
                }
                
                # Remove None values
                location = {k: v for k, v in location.items() if v is not None}
                
                return location if location else None
            
            return None

        except Exception as e:
            print(f"Error getting location info: {e}")
            print(traceback.format_exc())
            return None

    async def process_url(self, url: str) -> Optional[Dict]:
        """Process a TikTok URL and return location information."""
        try:
            print(f"Processing TikTok URL: {url}")
            
            # Extract video ID
            video_id = self.extract_video_id(url)
            if not video_id:
                print("Could not extract video ID")
                return None
            
            print(f"Video ID: {video_id}")
            
            # Get location information
            location = await self.get_location_info(video_id)
            
            if location:
                print("Location information found:")
                for key, value in location.items():
                    print(f"{key}: {value}")
                return location
            else:
                print("No location information found for this video")
                return None

        except Exception as e:
            print(f"Error processing URL: {e}")
            return None

async def main():
    # Example usage
    extractor = TikTokLocationExtractor()
    
    # Test with a real TikTok URL
    test_url = "https://www.tiktok.com/@rome.travelers/video/7185551271389072682?q=best%20restaurants%20in%20rome&t=1733264615391"
    result = await extractor.process_url(test_url)
    print("Result:", result)

if __name__ == "__main__":
    asyncio.run(main())