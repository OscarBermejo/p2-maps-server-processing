from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import sys

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.restaurants import Base

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# Create a new SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create all tables defined in the Base metadata
Base.metadata.create_all(engine)

print("Database tables created successfully.")