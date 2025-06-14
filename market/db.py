import os
from databases import Database
from sqlalchemy import MetaData
from dotenv import load_dotenv

DATABASE_URL = "postgresql+asyncpg://user1:useradmin@rc1d-0bvn2bd889klk39p.mdb.yandexcloud.net:6432/db1?sslmode=require"


if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

database = Database(DATABASE_URL)
metadata = MetaData()
