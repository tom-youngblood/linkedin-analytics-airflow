import sqlite3
import pandas as pd
import gspread
from dotenv import load_dotenv
import json
import base64
import os
import logging
from datetime import datetime

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

# Connect to SQLite database
logger.info("Connecting to SQLite database")
try:
    conn = sqlite3.connect('../data/post_scrapes.db')
    cursor = conn.cursor()
    logger.info("Successfully connected to SQLite database")
except Exception as e:
    logger.error(f"Failed to connect to SQLite database: {str(e)}")
    raise

# Create tables
logger.info("Creating database tables if they don't exist")
try:
    # Create posts table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        post_url TEXT PRIMARY KEY,
        post_name TEXT,
        last_scraped_at TEXT,
        scrape_count INTEGER,
        total_reactions INTEGER
    )
    """)
    logger.debug("Created/verified posts table")

    # Create scrapes table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scrapes (
        scrape_id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_url TEXT,
        ran_at TEXT,
        reactions_count INTEGER,
        cost REAL,
        status TEXT,
        FOREIGN KEY (post_url) REFERENCES posts(post_url)
    )
    """)
    logger.debug("Created/verified scrapes table")

    # Create engagers table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS engagers (
        engager_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrape_id INTEGER,
        linkedin_url TEXT,
        engagement_type TEXT,
        pushed_to_hubspot BOOLEAN DEFAULT 0,
        FOREIGN KEY (scrape_id) REFERENCES scrapes(scrape_id)
    );
    """)
    logger.debug("Created/verified engagers table")
except Exception as e:
    logger.error(f"Failed to create database tables: {str(e)}")
    raise

# Get existing post URLs before ingestion
cursor.execute("SELECT post_url FROM posts")
existing_urls = {row[0] for row in cursor.fetchall()}
logger.info(f"Found {len(existing_urls)} existing posts in database")

# Ingest posts and post_name from DF
logger.info("Starting post ingestion from Google Sheets data")
try:
    new_posts = 0
    updated_posts = 0
    
    for _, row in links.iterrows():
        is_new = row['link'] not in existing_urls
        cursor.execute("""
            INSERT INTO posts (post_url, post_name, last_scraped_at, scrape_count, total_reactions)
            VALUES (?, ?, NULL, 0, 0)
            ON CONFLICT(post_url) DO UPDATE SET post_name=excluded.post_name
        """, (row['link'], row['post']))
        
        if is_new:
            new_posts += 1
        else:
            updated_posts += 1
            
    conn.commit()
    logger.info(f"Post ingestion completed: {new_posts} new posts added, {updated_posts} existing posts updated")
except Exception as e:
    logger.error(f"Failed to ingest posts: {str(e)}")
    raise

# Close database connection
logger.info("Closing database connection")
conn.close()
logger.info("Script execution completed successfully")




