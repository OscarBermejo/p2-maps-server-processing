import asyncio
import os
import sys
from pathlib import Path
import time
# Add the src directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.download_video import extract_data

async def test_video_download():
    """Test video download functionality"""
    test_url = "https://www.tiktok.com/@rome.travelers/video/7185551271389072682"
    
    print("\nStarting video download test...")
    
    # Update the unpacking to match the actual number of returned values
    result = await extract_data(test_url)  # Store in a variable first to inspect
    print(f"Number of returned values: {len(result)}")  # Debug print
    video_id, video_file, audio_file, description, creator_info = result  # Added creator_info
    
    # Print results
    print("\nTest Results:")
    print(f"Video ID: {video_id}")
    print(f"Video File: {video_file}")
    print(f"Audio File: {audio_file}")
    print(f"Description: {description[:100]}..." if description else "No description found")
    print(f"Creator Info: {creator_info}")  # Added creator info printing
    
    # Verify files exist
    print("\nVerifying files:")
    print(f"Video file exists: {os.path.exists(video_file)}")
    print(f"Audio file exists: {os.path.exists(audio_file)}")
    
    # Print file sizes
    if os.path.exists(video_file):
        video_size = os.path.getsize(video_file) / (1024 * 1024)  # Convert to MB
        print(f"Video file size: {video_size:.2f} MB")
    
    if os.path.exists(audio_file):
        audio_size = os.path.getsize(audio_file) / (1024 * 1024)  # Convert to MB
        print(f"Audio file size: {audio_size:.2f} MB")
        
    return True
        


if __name__ == "__main__":
    # Record start time
    start_time = time.time()
    
    # Run the test
    success = asyncio.run(test_video_download())
    
    # Calculate and print elapsed time
    elapsed_time = time.time() - start_time
    print(f"\nTotal execution time: {elapsed_time:.2f} seconds")
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)