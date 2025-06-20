import pandas as pd
import gspread
from dotenv import load_dotenv
import json
import base64
import os
import logging
from datetime import datetime
import utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/gs_sql_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Load Environment variables
    logger.info("Loading environment variables")
    load_dotenv()

    # Get the encoded key from environment variable
    encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    logger.debug("Retrieved encoded service account key")

    # Decode the key
    gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
    logger.debug("Decoded service account key")

    # Connect to Google Sheets
    logger.info("Connecting to Google Sheets")
    try:
        gc = gspread.service_account_from_dict(gspread_credentials)
        links = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])
        logger.info(f"Retrieved {len(links)} links from Google Sheets")
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {str(e)}")
        raise

    # Connect to PostgreSQL database
    logger.info("Connecting to PostgreSQL database")
    try:
        conn = utils.get_db_connection()
        cursor = utils.get_db_cursor(conn)
        logger.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
        raise

    # Create tables
    logger.info("Creating database tables if they don't exist")
    try:
        # Create posts table
        cursor.execute("""
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
            image_url TEXT
        )
        """)
        logger.debug("Created/verified posts table")

        # Create scrapes table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_posts_scrapes (
            id SERIAL PRIMARY KEY,
            post_url TEXT,
            ran_at TIMESTAMP,
            reactions_count INTEGER,
            cost REAL,
            status TEXT,
            FOREIGN KEY (post_url) REFERENCES linkedin_posts(post_url)
        )
        """)
        logger.debug("Created/verified scrapes table")

        # Create engagers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_engagers (
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
        )
        """)
        logger.debug("Created/verified engagers table")
    except Exception as e:
        logger.error(f"Failed to create database tables: {str(e)}")
        raise

    # Get existing post URLs before ingestion
    existing_urls = utils.log_query_results(
        cursor,
        "Existing posts query",
        "SELECT post_url FROM linkedin_posts"
    )
    existing_urls = {row['post_url'] for row in existing_urls}
    logger.info(f"Found {len(existing_urls)} existing posts in database")

    # Ingest posts and post_name from DF
    logger.info("Starting post ingestion from Google Sheets data")
    try:
        new_posts = 0
        updated_posts = 0
        
        for _, row in links.iterrows():
            is_new = row['link'] not in existing_urls
            cursor.execute("""
                INSERT INTO linkedin_posts (post_url, post_name, last_scraped_at, scrape_count, total_reactions)
                VALUES (%s, %s, NULL, 0, 0)
                ON CONFLICT(post_url) DO UPDATE SET post_name=EXCLUDED.post_name
            """, (row['link'], row['post']))
            
            if is_new:
                new_posts += 1
            else:
                updated_posts += 1
                
        conn.commit()
        logger.info(f"Post ingestion completed: {new_posts} new posts added, {updated_posts} existing posts updated")

        # Log final state after ingestion
        utils.log_query_results(
            cursor,
            "Final posts count",
            "SELECT COUNT(*) as count FROM linkedin_posts"
        )
        utils.log_query_results(
            cursor,
            "Recent posts",
            "SELECT post_url, post_name, last_scraped_at, scrape_count FROM linkedin_posts ORDER BY id DESC LIMIT 5"
        )

    except Exception as e:
        logger.error(f"Failed to ingest posts: {str(e)}")
        raise

    # Close database connection
    logger.info("Closing database connection")
    cursor.close()
    conn.close()
    logger.info("Script execution completed successfully")

if __name__ == "__main__":
    main()
