"""
Database configuration for FastAPI application
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'password')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'medical_warehouse')}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class for models
Base = declarative_base()

# Dependency to get database session
def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def execute_query(query: str, params: dict = None):
    """Execute a raw SQL query and return results"""
    with engine.connect() as connection:
        result = connection.execute(text(query), params or {})
        return result.fetchall()

def execute_query_to_dataframe(query: str, params: dict = None):
    """Execute a query and return results as a list of dictionaries"""
    results = execute_query(query, params)
    if results:
        columns = results[0]._fields
        return [dict(row._mapping) for row in results]
    return []

# Test database connection
def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False