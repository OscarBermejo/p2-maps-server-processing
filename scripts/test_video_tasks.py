import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from src.tasks.video_tasks import process_video

def test_video_processing():
    # You can replace this URL with any TikTok video URL you want to test
    test_url = input("Please enter a TikTok video URL: ")
    
    logger.info(f"Starting test with URL: {test_url}")
    
    try:
        process_video(test_url)
        logger.info("Video processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error processing video: {str(e)}", exc_info=True)
        logger.error(f"Error type: {type(e)}")

if __name__ == "__main__":
    logger.info("=== Starting Video Processing Test ===")
    test_video_processing()
    logger.info("=== Test Complete ===")