from sqlalchemy import create_engine
from src.config import CONFIG

def get_db_connection():
    db = CONFIG["db"]
    return create_engine(f"mariadb+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}")
