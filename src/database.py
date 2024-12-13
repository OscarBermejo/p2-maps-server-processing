from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import DeclarativeBase
from src.utils.logger_config import setup_cloudwatch_logging
from decouple import config

logger = setup_cloudwatch_logging()

# Load environment variables from .env file
load_dotenv()

# Get database URL from individual environment variables
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_PORT = 3306

# Construct the database URL
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

if not SQLALCHEMY_DATABASE_URL:
    raise ValueError("Could not construct database URL from environment variables")

# Create SQLAlchemy engine with MariaDB-specific settings
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=5,  # Maximum number of database connections in the pool
    pool_recycle=3600,  # Recycle connections after 1 hour
    echo=True  # Set to False in production - this logs all SQL queries
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class using new style
class Base(DeclarativeBase):
    pass

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        logger.info("Database connection established")
        yield db
    finally:
        logger.info("Database connection closed")
        db.close()

# Initialize database (create all tables)
def init_db():
    from models.models import Base  # Adjust this import path based on your project structure
    Base.metadata.create_all(bind=engine)

# You can call this when starting your application
if __name__ == "__main__":
    init_db()