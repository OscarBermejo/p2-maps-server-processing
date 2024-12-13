import asyncio
import os
import sys
from pathlib import Path
# Add the src directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.download_video import extract_data

async def test_video_download():
    """Test video download functionality"""
    # Test URL - replace with a real social media video URL
    test_url = "https://www.tiktok.com/@itslucylouxo/video/7209696691778374918?q=best%20restaurant%20in%20amsterdam&t=1731145313755"
    
    print("\nStarting video download test...")
    
    # Download video and extract data
    video_id, video_file, audio_file, description = await extract_data(test_url)
    
    # Print results
    print("\nTest Results:")
    print(f"Video ID: {video_id}")
    print(f"Video File: {video_file}")
    print(f"Audio File: {audio_file}")
    print(f"Description: {description[:100]}..." if description else "No description found")
    
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
    # Run the test
    success = asyncio.run(test_video_download())
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)