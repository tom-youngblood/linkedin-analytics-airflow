"""
This script is responsible for setting up the entire database schema idempotently.
It ensures that all required tables and columns exist, making the pipeline's
database environment consistent and self-contained.

This should be the first task executed in the Airflow DAG.
"""

import logging
from utils import get_db_connection, get_db_cursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/setup_database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# A list of all DDL (Data Definition Language) queries to set up the schema.
# Using "IF NOT EXISTS" makes these operations idempotent.
DDL_QUERIES = [
    # 1. Create linkedin_posts table
    """
    CREATE TABLE IF NOT EXISTS linkedin_posts (
        id SERIAL PRIMARY KEY,
        post_url TEXT UNIQUE,
        post_name TEXT,
        last_scraped_at TIMESTAMP,
        scrape_count INTEGER DEFAULT 0,
        total_reactions INTEGER DEFAULT 0,
        text TEXT,
        post_type TEXT,
        comments INTEGER,
        reposts INTEGER,
        reshared_post_url TEXT,
        reshared_post_total_reactions INTEGER,
        media_type TEXT,
        media_url TEXT,
        article_url TEXT,
        article_title TEXT,
        duration REAL,
        mime_type TEXT,
        thumbnail TEXT,
        video_url TEXT,
        image_url TEXT,
        enriched BOOLEAN DEFAULT FALSE,
        enriched_time TIMESTAMP
    );
    """,
    # 2. Add new marketing/content columns to linkedin_posts
    "ALTER TABLE linkedin_posts ADD COLUMN IF NOT EXISTS sponsored INTEGER;",
    "ALTER TABLE linkedin_posts ADD COLUMN IF NOT EXISTS time_to_create INTEGER;",
    "ALTER TABLE linkedin_posts ADD COLUMN IF NOT EXISTS sentiment TEXT;",
    "ALTER TABLE linkedin_posts ADD COLUMN IF NOT EXISTS target_audience TEXT;",
    "ALTER TABLE linkedin_posts ADD COLUMN IF NOT EXISTS video_type TEXT;",

    # 3. Create linkedin_posts_scrapes table
    """
    CREATE TABLE IF NOT EXISTS linkedin_posts_scrapes (
        id SERIAL PRIMARY KEY,
        post_url TEXT,
        ran_at TIMESTAMP,
        reactions_count INTEGER,
        cost REAL,
        status TEXT,
        FOREIGN KEY (post_url) REFERENCES linkedin_posts(post_url)
    );
    """,
    # 4. Rename old table if it exists
    "ALTER TABLE IF EXISTS linkedin_engagers RENAME TO linkedin_engagers_by_post;",

    # 5. Create the new, simplified linkedin_engagers_by_post table
    """
    CREATE TABLE IF NOT EXISTS linkedin_engagers_by_post (
        id SERIAL PRIMARY KEY,
        scrape_id INTEGER,
        linkedin_url TEXT,
        name TEXT,
        headline TEXT,
        engagement_type TEXT,
        post_url TEXT,
        pushed_to_hubspot BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (scrape_id) REFERENCES linkedin_posts_scrapes(id),
        UNIQUE(linkedin_url, post_url)
    );
    """,
    
    # 6. Drop old enrichment columns from the renamed table if they exist
    "ALTER TABLE linkedin_engagers_by_post DROP COLUMN IF EXISTS company;",
    "ALTER TABLE linkedin_engagers_by_post DROP COLUMN IF EXISTS title;",
    "ALTER TABLE linkedin_engagers_by_post DROP COLUMN IF EXISTS engager_audience;",
    "ALTER TABLE linkedin_engagers_by_post DROP COLUMN IF EXISTS engager_bucketed_position;",

    # 7. Drop the redundant companies table
    "DROP TABLE IF EXISTS linkedin_companies;"
]

def main():
    """
    Connects to the database and executes all DDL queries to ensure the
    schema is correctly set up.
    """
    logger.info("Starting database schema setup...")
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        logger.info(f"Executing {len(DDL_QUERIES)} DDL queries...")
        for query in DDL_QUERIES:
            # Log the specific query being executed for better traceability
            # (log a shortened version for readability)
            log_query = ' '.join(query.splitlines()).strip()[:100] + "..."
            logger.info(f"Running: {log_query}")
            cursor.execute(query)

        conn.commit()
        logger.info("All DDL queries executed successfully. Database schema is up to date.")

    except Exception as e:
        logger.error(f"An error occurred during database setup: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
