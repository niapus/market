from databases import Database
from sqlalchemy import MetaData

DB_HOST = "rc1d-0bvn2bd889klk39p.mdb.yandexcloud.net"
DB_PORT = "6432"
DB_NAME = "db1"
DB_USER = "user1"
DB_PASSWORD = "useradmin"

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=require"

database = Database(DATABASE_URL)
metadata = MetaData()