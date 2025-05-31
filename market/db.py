import os
from databases import Database
from sqlalchemy import MetaData
from dotenv import load_dotenv

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

database = Database(DATABASE_URL)
metadata = MetaData()
