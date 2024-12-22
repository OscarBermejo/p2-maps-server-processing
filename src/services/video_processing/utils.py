from typing import Dict, Optional, Union
import re
import googlemaps
import openai
import sys
import os
from openai.types.chat import ChatCompletion
from decouple import config  # Add this import at the top
import json
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
#from src.utils.logger_config import setup_cloudwatch_logging
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

logger = logging.getLogger(__name__)  

gmaps = googlemaps.Client(key=config('GOOGLE_MAPS_API_KEY'))

def test_database_connection():
    """Test database connection and permissions"""
    try:
        # Get database credentials from environment
        db_host = config('DB_HOST')  # IP of p2-maps-server-II
        db_name = config('DB_NAME')
        db_user = config('DB_USER')
        db_password = config('DB_PASSWORD')
        db_port = 3306  # Remove comment from the actual value
        
        # Create connection URL
        # For MySQL:
        db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Create engine
        engine = create_engine(db_url)
        
        # Test connection and retrieve data
        with engine.connect() as connection:
            print("Successfully connected to database")
            
            # Test basic connection
            connection.execute(text("SELECT 1"))
            print("Successfully executed test query")
            
            # Retrieve restaurants in Antwerp
            query = text("SELECT * FROM restaurants WHERE city = 'Antwerpen'")
            result = connection.execute(query)
            
            # Fetch and print results
            restaurants = result.fetchall()
            print("\nRestaurants in Antwerp:")
            for restaurant in restaurants:
                print(f"Restaurant: {restaurant._mapping}")
            
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        return False

def query_chatgpt(description: str, text: str, transcription: str) -> str:
    """
    Query ChatGPT to extract places of interest from video information.
    
    Args:
        description: Video description
        text: Extracted text from video
        transcription: Video transcription
    
    Returns:
        str: Formatted recommendations or "No places of interest found"
    """
    logger.info("Querying ChatGPT for places of interest")
    if not any([description, text, transcription]):
        logger.warning("All input parameters are empty")
        return "No places of interest found"

    chatgpt_query = f"""
        Analyze the following information from a TikTok video and identify recommended places:
        
        Description: {description}
        Transcription: {transcription}
        Text in images: {text}
        
        Instructions:
        1. Return only specific places that are being explicitly recommended or reviewed
        2. Format each place as: [Place Name], [City] (if not in any city use the closest city), [Type of Place]
        3. One place per line
        4. If no specific place is mentioned, return exactly: "No places of interest found"
        
        Example format:
        Maseria Moroseta, Ostuni, Boutique Hotel
        Grotta Palazzese, Polignano, Restaurant

        Notes: The city name must be in english.
    """

    # Initialize OpenAI client with API key
    openai.api_key = config('OPENAI_API_KEY')
    
    try:
        response: ChatCompletion = openai.chat.completions.create(
            model="gpt-4-turbo-preview",  # Updated to latest model
            messages=[{"role": "user", "content": chatgpt_query}],
            max_tokens=150,
            temperature=0.3  # Reduced for more consistent outputs
        )
        logger.info("Successfully queried ChatGPT")
        return response.choices[0].message.content.strip()
    except openai.APIError as e:
        logger.error(f"OpenAI API error: {str(e)}", exc_info=True)
        return "No places of interest found"

def search_location(recommendations: str) -> Dict[str, Dict]:
    """
    Search for places using Google Maps API and return their details.
    """
    logger.info("Searching for locations using Google Maps API")
    if not recommendations or "No places of interest found" in recommendations:
        logger.warning("No valid recommendations to search")
        return {}

    google_map_dict = {}
    places = [place.strip() for place in recommendations.splitlines() if place.strip()]
    logger.info(f'Places recommended: {places}')

    for location in places:
        try:
            # Split location string and extract city
            location_parts = location.split(',')
            place_name = location_parts[0].strip()
            city = location_parts[1].strip() if len(location_parts) > 1 else 'Unknown'
            
            # First get basic place info
            result = gmaps.places(query=location)
            if result['status'] == 'OK':
                place = result['results'][0]
                place_id = place['place_id']
                
                # Get detailed place information
                place_details = gmaps.place(place_id, fields=[
                    'name',
                    'formatted_address',
                    'geometry/location',
                    'rating',
                    'price_level',
                    'formatted_phone_number',
                    'opening_hours',
                    'website',
                    'user_ratings_total',
                    'url'  # For Google Maps link
                ])['result']
                
                location_info = {
                    'name': place['name'],
                    'address': place.get('formatted_address', 'No address found'),
                    'city': city,  # Using city from ChatGPT output
                    'latitude': place['geometry']['location']['lat'],
                    'longitude': place['geometry']['location']['lng'],
                    'google_maps_link': place_details.get('url', ''),
                    'rating': place_details.get('rating', 'No rating'),
                    'total_ratings': place_details.get('user_ratings_total', 0),
                    'price_level': {
                        0: 'Free',
                        1: '$',
                        2: '$$',
                        3: '$$$',
                        4: '$$$$'
                    }.get(place_details.get('price_level'), 'Price not available'),
                    'phone': place_details.get('formatted_phone_number', 'No phone number'),
                    'website': place_details.get('website', 'No website'),
                }
                
                # Add opening hours if available
                if 'opening_hours' in place_details:
                    location_info['is_open_now'] = place_details['opening_hours'].get('open_now')
                    location_info['hours'] = place_details['opening_hours'].get('weekday_text', [])
                
                google_map_dict[location] = location_info
                logger.info(f"Successfully found location for: {location}")
            else:
                logger.warning(f"No results found for location: {location}")
        
        except Exception as e:
            logger.error(f"Error searching location '{location}': {str(e)}")
            continue

    return google_map_dict

def store_video_data(video_id: str, url: str, creator_info: dict, description: str, 
                     text_data: str, audio_data: str, recommendations: str, places_data: dict) -> None:
    """
    Store video processing results in both S3 and database.
    
    Args:
        video_id: Unique identifier for the video
        url: Original video URL
        creator_info: Dictionary containing creator information
        description: Video description
        text_data: Extracted text from video
        audio_data: Transcribed audio data
        recommendations: Processed recommendations from ChatGPT
        places_data: Dictionary containing place details
    """
    # Prepare data structure
    extracted_data = {
        "video_id": video_id,
        "platform": "tiktok",
        "video_url": url,
        "creator_info": creator_info,
        "extracted_data": {
        "description": description,
        "text_data": text_data,
        "audio_data": audio_data,
        "recommendations": recommendations
        },
        "places_data": places_data,
        "processed_at": datetime.utcnow().isoformat()
    }

    # Save to S3
    try:
        s3_client = boto3.client('s3')
        bucket_name = config('AWS_S3_BUCKET')
        s3_key = f'video_data/{video_id}.json'
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(extracted_data, ensure_ascii=False),
            ContentType='application/json'
        )
        print(f"Successfully saved data to S3: s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"Error saving to S3: {str(e)}")

    # Save to database
    try:
        from src.utils.database_utils import update_database
        update_database(
            video_id=video_id,
            platform="tiktok",
            video_url=url,
            creator_info=creator_info,
            places_data=places_data
        )
        print("Successfully updated database")
    except Exception as e:
        print(f"Error updating database: {str(e)}")

