import whisper
import os
import sys
import warnings 
import time
from ...utils.logger_config import setup_cloudwatch_logging
import logging

logger = logging.getLogger(__name__)

# Suppress all warnings
warnings.filterwarnings("ignore")

class AudioExtractor:
    def __init__(self, model_name='small'):
        logger.info(f"Initializing AudioExtractor with model: {model_name}")
        self.model_name = model_name

    def transcribe_audio(self, audio_path):
        logger.info(f"Starting audio transcription for: {audio_path}")
        try:
            if not os.path.isfile(audio_path):
                logger.error(f"Audio file not found: {audio_path}")
                return ""

            file_size = os.path.getsize(audio_path) / (1024 * 1024)
            logger.info(f"Audio file size: {file_size:.2f} MB")

            start_time = time.time()
            model = whisper.load_model(self.model_name)
            logger.info(f"Model loaded in {time.time() - start_time:.2f} seconds")

            transcription_start = time.time()
            result = model.transcribe(audio_path, verbose=True)
            transcription_time = time.time() - transcription_start
            logger.info(f"Transcription completed in {transcription_time:.2f} seconds")

            text = result['text'].strip()
            logger.info(f"Transcription result length: {len(text)} characters")
            return text

        except Exception as e:
            logger.error(f"Transcription failed: {str(e)}", exc_info=True)
            return ""

def main(audio_path):
    extractor = AudioExtractor()
    return extractor.transcribe_audio(audio_path)

if __name__ == "__main__":
    main()