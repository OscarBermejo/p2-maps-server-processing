import os
import time
import cv2
import uuid
import logging
from pathlib import Path
from paddleocr import PaddleOCR
from moviepy.editor import VideoFileClip
from ...utils.logger_config import setup_cloudwatch_logging

logger = logging.getLogger(__name__)

class TextExtractor:
    def __init__(self):
        logger.info("Initializing PaddleOCR TextExtractor")
        self.ocr = PaddleOCR(
            use_angle_cls=True,
            lang='en',
            show_log=False
        )

    def get_video_length(self, video_path):
        clip = VideoFileClip(video_path)
        duration = clip.duration
        clip.close()
        return duration

    def extract_frames(self, video_path, sample_rate=1):
        """Extract frames from video at given sample rate (frames per second)"""
        logger.info(f"Extracting frames from video: {video_path}")
        try:
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_interval = int(fps / sample_rate)
            frames = []
            count = 0

            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                if count % frame_interval == 0:
                    frames.append(frame)
                count += 1

            cap.release()
            logger.info(f"Extracted {len(frames)} frames from video")
            return frames

        except Exception as e:
            logger.error(f"Frame extraction failed: {str(e)}", exc_info=True)
            raise

    def extract_text(self, video_file_path, video_id):
        """Main method to extract text from video, maintaining same interface"""
        logger.info(f"Starting text extraction for video ID: {video_id}")
        try:
            start_time = time.time()
            
            # Verify file exists
            if not os.path.exists(video_file_path):
                logger.error(f"Video file not found: {video_file_path}")
                return ""
            
            # Get video length
            video_length = self.get_video_length(video_file_path)
            logger.info(f"Video length: {video_length:.2f} seconds")
            
            # Extract frames
            frames = self.extract_frames(video_file_path)
            logger.info(f"Processing {len(frames)} frames")
            
            # Process frames and extract text
            extracted_texts = set()
            for i, frame in enumerate(frames):
                try:
                    # Run OCR on frame
                    results = self.ocr.ocr(frame)
                    
                    # Extract text from results
                    if results:
                        for line in results:
                            for detection in line:
                                text = detection[1][0]  # Get text content
                                confidence = detection[1][1]  # Get confidence score
                                if confidence > 0.5:  # Only keep high-confidence results
                                    extracted_texts.add(text)
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Processed {i + 1}/{len(frames)} frames")
                
                except Exception as e:
                    logger.warning(f"Error processing frame {i}: {str(e)}")
                    continue
            
            # Combine results
            result = '\n'.join(extracted_texts)
            
            end_time = time.time()
            processing_time = end_time - start_time
            logger.info(f"Text extraction completed in {processing_time:.2f} seconds. Found {len(extracted_texts)} unique texts")
            
            return result

        except Exception as e:
            logger.error(f"Text extraction failed: {str(e)}", exc_info=True)
            raise

def main(video_file_path, video_id):
    extractor = TextExtractor()
    return extractor.extract_text(video_file_path, video_id)

if __name__ == "__main__":
    main()