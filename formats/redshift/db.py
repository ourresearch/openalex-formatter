import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

database_url = os.getenv("REDSHIFT_SERVERLESS_URL")

if not database_url:
    raise ValueError("REDSHIFT_SERVERLESS_URL environment variable must be set")

engine = create_engine(database_url)
Session = sessionmaker(bind=engine)
Base = declarative_base()