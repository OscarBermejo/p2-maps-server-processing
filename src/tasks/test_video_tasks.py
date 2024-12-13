import sys
import os
import logging

# Add the project root to Python path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from video_tasks import process_video

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_video_processing():
    """
    Manual test function for video processing
    """
    # Get TikTok URL from user input
    tiktok_url = input("Please enter a TikTok URL to process: ")
    
    print(f"\nStarting to process video: {tiktok_url}")
    try:
        # Call the process_video function
        result = process_video(tiktok_url)
        print("\nVideo processing completed successfully!")
        print(f"Result: {result}")
        
    except Exception as e:
        print(f"\nError processing video: {str(e)}")
        raise

if __name__ == "__main__":
    test_video_processing()