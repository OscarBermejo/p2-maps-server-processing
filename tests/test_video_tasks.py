import sys
import os
from pathlib import Path
import logging
import asyncio
import concurrent.futures

# Add the parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.download_video import VideoDownloader
from src.services.video_processing.extract_audio import AudioExtractor
from src.services.video_processing.extract_text_paddleocr import TextExtractor
from src.services.video_processing.utils import query_chatgpt, search_location, store_video_data

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add console handler to logger
logger.addHandler(console_handler)

async def process_video(url: str):
    try:
        # 1. Download video and get metadata
        logger.info("Starting video download...")
        video_id, video_file, audio_file, description, creator_info = await VideoDownloader().process(url)
        logger.info(f"Download completed. Video ID: {video_id}")
        
        # 2. Extract audio and text in parallel using ThreadPoolExecutor
        logger.info("Starting parallel audio and text extraction...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            # Submit both tasks
            audio_future = executor.submit(
                AudioExtractor().transcribe_audio, 
                audio_file
            )
            text_future = executor.submit(
                TextExtractor().extract_text,
                video_file,
                video_id
            )
            
            # Wait for both tasks to complete
            completed_tasks = concurrent.futures.wait(
                [audio_future, text_future],
                return_when=concurrent.futures.ALL_COMPLETED
            )

            # Get audio data
            try:
                audio_data = audio_future.result()
                logger.info(f"Audio extraction completed. Length: {len(audio_data)}")
                logger.debug(f"Audio data: {audio_data[:100]}...")
            except Exception as e:
                logger.error(f"Audio extraction failed: {str(e)}", exc_info=True)
                audio_data = ""
            
            # Get text data
            try:
                text_data = text_future.result()
                logger.info(f"Text extraction completed. Length: {len(text_data)}")
                logger.debug(f"Extracted text: {text_data[:100]}...")
            except Exception as e:
                logger.error(f"Text extraction failed: {str(e)}", exc_info=True)
                text_data = ""
        
        # 3. Extract location from text
        logger.info("Starting ChatGPT query...")
        recommendations = query_chatgpt(description, text_data, audio_data)
        logger.info(f"ChatGPT query completed: {recommendations}")
        
        # 4. Get coordinates and place details
        places_data = search_location(recommendations)
        logger.info(f"Location search completed: {places_data}")
        
        # 5. Store all data
        store_video_data(
            video_id=video_id,
            url=url,
            creator_info=creator_info,
            description=description,
            text_data=text_data,
            audio_data=audio_data,
            recommendations=recommendations,
            places_data=places_data
        )

        # 6. Cleanup files
        logger.info(f"Cleaning up temporary files for video {video_id}")
        
        # Define file paths
        video_file = f"/home/ec2-user/maps-server-processing/files/video/{video_id}.mp4"
        audio_file = f"/home/ec2-user/maps-server-processing/files/audio/{video_id}.wav"
        
        # Remove video file
        if os.path.exists(video_file):
            os.remove(video_file)
            logger.info(f"Removed video file: {video_file}")
            
        # Remove audio file
        if os.path.exists(audio_file):
            os.remove(audio_file)
            logger.info(f"Removed audio file: {audio_file}")

    except Exception as e:
        logger.error(f"Major error in process_video: {str(e)}", exc_info=True)
        logger.error(f"Error type: {type(e)}")
        raise

if __name__ == "__main__":

    url = "https://www.tiktok.com/@rome.travelers/video/7185551271389072682"
    asyncio.run(process_video(url))