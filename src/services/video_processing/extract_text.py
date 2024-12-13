import boto3
import subprocess
import time
import botocore.exceptions
import os
import uuid
from moviepy.editor import VideoFileClip
import asyncio
import aioboto3
from ...utils.logger_config import setup_cloudwatch_logging
import logging

logger = logging.getLogger(__name__)

class TextExtractor:
    def __init__(self, aws_region='eu-central-1'):
        logger.info(f"Initializing TextExtractor with region: {aws_region}")
        # AWS configuration
        self.aws_region = aws_region
        self.kinesis_stream_name = 'p2-maps-server'
        self.bucket_name = 'p2-maps-server'

        # Initialize AWS clients
        self.kinesis_client = boto3.client('kinesisvideo', region_name=aws_region)
        self.rekognition_client = boto3.client('rekognition', region_name=aws_region)
        self.s3_client = boto3.client('s3', region_name=aws_region)

    def create_kinesis_stream_if_not_exists(self, stream_name):
        logger.info(f"Checking Kinesis stream existence: {stream_name}")
        try:
            self.kinesis_client.describe_stream(StreamName=stream_name)
            print(f"Kinesis Video Stream '{stream_name}' already exists.")
            logger.info(f"Kinesis stream '{stream_name}' verified/created successfully")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.kinesis_client.create_stream(
                    StreamName=stream_name,
                    DataRetentionInHours=24
                )
                print(f"Created Kinesis Video Stream: {stream_name}")
                while True:
                    response = self.kinesis_client.describe_stream(StreamName=stream_name)
                    if response['StreamInfo']['Status'] == 'ACTIVE':
                        print(f"Stream '{stream_name}' is now active.")
                        break
                    time.sleep(1)
            else:
                raise

    def get_data_endpoint(self, stream_name, api_name):
        response = self.kinesis_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

    def upload_video_to_s3(self, video_file_path, object_name):
        logger.info(f"Uploading video to S3: {object_name}")
        try:
            self.s3_client.upload_file(video_file_path, self.bucket_name, object_name)
            print(f"Uploaded video to S3: s3://{self.bucket_name}/{object_name}")
            logger.info(f"Successfully uploaded video to S3: {object_name}")
        except Exception as e:
            print(f"Failed to upload video to S3: {e}")
            logger.error(f"S3 upload failed: {str(e)}", exc_info=True)
            raise

    def start_text_detection_s3(self, object_name, video_id):
        unique_token = f"{video_id}-{uuid.uuid4()}"
        response = self.rekognition_client.start_text_detection(
            Video={
                'S3Object': {
                    'Bucket': self.bucket_name,
                    'Name': object_name
                }
            },
            ClientRequestToken=unique_token
        )
        return response['JobId']

    def get_text_detection_results(self, job_id):
        while True:
            response = self.rekognition_client.get_text_detection(JobId=job_id)
            status = response['JobStatus']
            print(f"Job Status: {status}")
            if status == 'SUCCEEDED':
                return response['TextDetections']
            elif status == 'FAILED':
                raise Exception(f"Text detection job failed: {response.get('StatusMessage', 'No status message')}")
            time.sleep(2)

    def convert_video_for_rekognition(self, input_path):
        try:
            print(f"[DEBUG] Starting video conversion from: {input_path}")
            output_path = input_path.rsplit('.', 1)[0] + '_converted.mp4'
            print(f"[DEBUG] Will convert to: {output_path}")
            
            command = [
                'ffmpeg',
                '-y',  # Overwrite output file if exists
                '-loglevel', 'quiet',  # Suppress FFmpeg output
                '-i', input_path,
                '-c:v', 'libx264',
                '-preset', 'ultrafast',
                '-crf', '28',
                # Ensure dimensions are even numbers
                '-vf', 'scale=720:1280:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2',
                '-c:a', 'aac',
                '-b:a', '128k',
                '-movflags', '+faststart',
                output_path
            ]
            
            print("[DEBUG] Running FFmpeg command...")
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Wait for process to complete
            _, stderr = process.communicate()
            
            # Check if conversion was successful
            if process.returncode != 0:
                raise Exception(f"FFmpeg conversion failed with return code {process.returncode}. Error: {stderr}")
            
            if not os.path.exists(output_path):
                raise Exception("Converted file was not created")
            
            print(f"[DEBUG] Video conversion completed: {output_path}")
            print(f"[DEBUG] Output file size: {os.path.getsize(output_path) / (1024*1024):.2f} MB")
            return output_path
            
        except Exception as e:
            print(f"[ERROR] Video conversion failed: {str(e)}")
            print(f"[ERROR] Error type: {type(e)}")
            raise

    def get_video_length(self, video_path):
        clip = VideoFileClip(video_path)
        duration = clip.duration
        clip.close()
        return duration

    def delete_from_s3(self, object_name):
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_name)
            print(f"Deleted video from S3: s3://{self.bucket_name}/{object_name}")
        except Exception as e:
            print(f"Failed to delete video from S3: {e}")
            raise

    def extract_text(self, video_file_path, video_id):
        logger.info(f"Starting text extraction for video ID: {video_id}")
        try:
            print(f"[DEBUG] Starting text extraction for video: {video_id}")
            start_time = time.time()
            
            print(f"[DEBUG] Checking video file exists: {video_file_path}")
            if not os.path.exists(video_file_path):
                print(f"[ERROR] Video file not found: {video_file_path}")
                return ""
            
            # Get video length
            print("[DEBUG] Getting video length...")
            video_length = self.get_video_length(video_file_path)
            logger.info(f"Video length: {video_length:.2f} seconds")
            
            # Convert video
            print("[DEBUG] Converting video for Rekognition...")
            converted_video_path = self.convert_video_for_rekognition(video_file_path)
            print(f"[DEBUG] Video converted: {converted_video_path}")
            
            object_name = f'videos/{os.path.basename(converted_video_path)}'
            print(f"[DEBUG] S3 object name: {object_name}")

            # Ensure Kinesis stream exists
            print("[DEBUG] Checking Kinesis stream...")
            try:
                self.create_kinesis_stream_if_not_exists(self.kinesis_stream_name)
                print("[DEBUG] Kinesis stream check completed")
            except Exception as e:
                print(f"[ERROR] Kinesis stream error: {str(e)}")
                raise

            # Upload to S3
            print("[DEBUG] Uploading video to S3...")
            try:
                self.upload_video_to_s3(converted_video_path, object_name)
                print("[DEBUG] Upload completed")
            except Exception as e:
                print(f"[ERROR] S3 upload error: {str(e)}")
                raise

            # Start text detection
            print("[DEBUG] Starting text detection...")
            try:
                job_id = self.start_text_detection_s3(object_name, video_id)
                print(f"[DEBUG] Text detection job started: {job_id}")
            except Exception as e:
                print(f"[ERROR] Text detection start error: {str(e)}")
                raise

            # Get results
            print("[DEBUG] Getting text detection results...")
            try:
                text_detections = self.get_text_detection_results(job_id)
                print(f"[DEBUG] Got {len(text_detections)} text detections")
            except Exception as e:
                print(f"[ERROR] Text detection results error: {str(e)}")
                raise

            # Process results
            print("[DEBUG] Processing text detections...")
            extracted_texts = set()
            for detection in text_detections:
                text = detection['TextDetection']['DetectedText']
                extracted_texts.add(text)
            
            result = '\n'.join(extracted_texts)
            print(f"[DEBUG] Processed {len(extracted_texts)} unique texts")

            # Cleanup
            print("[DEBUG] Starting cleanup...")
            try:
                os.remove(converted_video_path)
                self.delete_from_s3(object_name)
                print("[DEBUG] Cleanup completed")
            except Exception as e:
                print(f"[ERROR] Cleanup error: {str(e)}")

            end_time = time.time()
            total_processing_time = end_time - start_time
            print(f"[DEBUG] Total text extraction time: {total_processing_time:.2f} seconds")

            logger.info(f"Text extraction completed. Found {len(extracted_texts)} unique texts")
            return result

        except Exception as e:
            logger.error(f"Text extraction failed: {str(e)}", exc_info=True)
            raise

def main():
    extractor = TextExtractor()
    video_file_path = '/home/ec2-user/maps-server/files/video/7185551271389072682.mp4'
    video_id = '7185551271389072682'
    return extractor.extract_text(video_file_path, video_id)

if __name__ == "__main__":
    main()
