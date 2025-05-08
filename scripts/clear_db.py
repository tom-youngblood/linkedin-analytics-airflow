import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/clear_db_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clear_database():
    try:
        # Connect to database
        conn = sqlite3.connect('../data/post_scrapes.db')
        cursor = conn.cursor()
        logger.info("Connected to database")

        # Disable foreign keys temporarily
        cursor.execute("PRAGMA foreign_keys = OFF;")

        # Clear all tables
        cursor.execute("DELETE FROM engagers;")
        cursor.execute("DELETE FROM scrapes;")
        cursor.execute("DELETE FROM posts;")

        # Reset autoincrement counters
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('engagers', 'scrapes', 'posts');")

        # Re-enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Commit changes
        conn.commit()
        logger.info("Database cleared successfully")

    except Exception as e:
        logger.error(f"Error clearing database: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    clear_database()