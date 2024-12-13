from src.database import Base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Table, Boolean
from sqlalchemy.orm import relationship
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from ..utils.logger_config import setup_cloudwatch_logging

logger = setup_cloudwatch_logging()

# Association table for restaurant tags
restaurant_tags = Table('restaurant_tags', Base.metadata,
    Column('restaurant_id', Integer, ForeignKey('restaurants.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class Restaurant(Base):
    __tablename__ = 'restaurants'
    # Stores restaurant information: name, location, rating, etc.
    # Has relationships with videos and tags

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    location = Column(String(255), nullable=False)
    city = Column(String(255), nullable=False)
    location_link = Column(String(255), nullable=False)
    restaurant_type = Column(String(255), nullable=True)
    coordinates = Column(String(255))
    rating = Column(Float, nullable=True)
    price_level = Column(Integer, nullable=True)  # 1-4 for $ to $$$$
    website = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    videos = relationship("Video", back_populates="restaurant")
    tags = relationship("Tag", secondary=restaurant_tags, back_populates="restaurants")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.info(f"New Restaurant instance created: {self.name}")

class Video(Base):
    __tablename__ = 'videos'
    # Stores video information: platform, URL, creator info
    # Links to a restaurant through restaurant_id

    id = Column(Integer, primary_key=True)
    platform = Column(String(50), nullable=False)  # e.g., "tiktok", "instagram", "youtube"
    video_id = Column(String(255), nullable=False)
    video_url = Column(String(500), nullable=True)
    creator_name = Column(String(255), nullable=False)
    creator_id = Column(String(255), nullable=False)
    view_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Foreign Keys
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'))
    # Relationships
    restaurant = relationship("Restaurant", back_populates="videos")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.info(f"New Video instance created for restaurant_id: {self.restaurant_id}")

class Tag(Base):
    __tablename__ = 'tags'
    # Stores categories/tags for restaurants
    # Example: "Italian", "Outdoor Seating", etc.

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    # Relationships
    restaurants = relationship("Restaurant", secondary=restaurant_tags, back_populates="tags")

# Pydantic Models for API
class TagSchema(BaseModel):
    # Defines how tag data should look in API responses
    id: int
    name: str

    class Config:
        from_attributes = True

class VideoSchema(BaseModel):
    # Defines how video data should look in API responses
    id: int
    platform: str
    video_id: str
    video_url: Optional[str]
    creator_name: str
    creator_id: str
    view_count: Optional[int]
    created_at: datetime
    restaurant_id: int

    class Config:
        from_attributes = True

class RestaurantSchema(BaseModel):
    # Defines how restaurant data should look in API responses
    id: int
    name: str
    location: str
    city: str
    location_link: str
    restaurant_type: Optional[str] = None
    coordinates: str
    rating: Optional[float] = None
    price_level: Optional[int] = None
    website: Optional[str] = None
    phone: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    videos: Optional[List[VideoSchema]] = []
    tags: Optional[List[TagSchema]] = []

    class Config:
        from_attributes = True

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Add Pydantic schemas for authentication
class UserSchema(BaseModel):
    # Defines how user data should look in API responses
    # Excludes sensitive info like password
    id: int
    email: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class UserCreateSchema(BaseModel):
    email: str
    password: str

class UserLoginSchema(BaseModel):
    # Defines the expected format for login requests
    email: str
    password: str
