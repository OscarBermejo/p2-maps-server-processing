import sys
import os
from pathlib import Path
import logging

# Add the parent directory to Python path to import the extract_text module
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.extract_text_paddleocr import main as extract_text
from src.utils.logger_config import setup_cloudwatch_logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
setup_cloudwatch_logging()

# Define test video path - adjust this to your actual test video location
test_video_path = '/home/ec2-user/maps-server-processing/files/video/7185551271389072682.mp4'
video_id = '7185551271389072682'

# Make sure the test video exists
if not os.path.exists(test_video_path):
    raise FileNotFoundError(f"Please place a test video at: {test_video_path}")

try:
    # Extract text from the video
    extracted_texts = extract_text(test_video_path, video_id)
    
    if extracted_texts is None:
        print("No texts were extracted from the video")
        sys.exit(1)
    
    print(extracted_texts)

except Exception as e:
    print(f"Error during text extraction: {str(e)}")
    raise

