import sys
import os
from pathlib import Path

# Add the parent directory to Python path to import the extract_text module
sys.path.append(str(Path(__file__).parent.parent))

from src.services.video_processing.extract_audio import main as extract_audio

# Define test video path - adjust this to your actual test video location
test_video_path = str(Path(__file__).parent.parent / 'files' / 'audio' / '7402402958266076432.wav')
print(test_video_path)
video_id = '7402402958266076432'

transcription = extract_audio(test_video_path)
print(transcription)