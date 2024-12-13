import sys
import os
from pathlib import Path

# Add the parent directory to Python path to import the extract_text module
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.extract_text import main as extract_text

# Define test video path - adjust this to your actual test video location
test_video_path = str(Path(__file__).parent.parent / 'files' / 'video' / '7185551271389072682.mp4')
video_id = '7185551271389072682'

# Ensure the test video directory exists
Path(test_video_path).parent.mkdir(parents=True, exist_ok=True)

# Make sure the test video exists
if not os.path.exists(test_video_path):
    raise FileNotFoundError(f"Please place a test video at: {test_video_path}")

try:
    # Extract text from the video
    extracted_texts = extract_text()
    
    if extracted_texts is None:
        print("No texts were extracted from the video")
        sys.exit(1)
        
    # Print results
    print("\nExtracted texts:")
    for i, text in enumerate(extracted_texts, 1):
        print(f"{i}. {text}")
    
    print(extracted_texts)

except Exception as e:
    print(f"Error during text extraction: {str(e)}")
    raise

