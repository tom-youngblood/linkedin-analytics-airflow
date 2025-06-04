# Query database to get top 3 posts to scrape: select P.post_name, P.post_url, S.ran_at, s_recent (window function to get the date)
# Scrape top three posts with APIfy
# Update scrapes table
# Send posts to hubspot

from dotenv import load_dotenv
import os
import pandas as pd
import utils
import logging
from datetime import datetime
import random
import time
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/scrape_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Load environment variables
    logger.info("Loading environment variables")
    load_dotenv() 

    # Connect to PostgreSQL DB
    conn = utils.get_db_connection()
    cursor = utils.get_db_cursor(conn)
    logger.info("Connected to DB")

    # Query: Scrape most recent posts 3 times
    posts_to_scrape = utils.log_query_results(
        cursor,
        "Posts to scrape query",
        """
        SELECT p.post_url
        FROM linkedin_posts p
        WHERE p.last_scraped_at IS NULL OR p.scrape_count < 3
        ORDER BY id DESC
        LIMIT 5
        """
    )
    logger.info("Queried five most recent posts with scrape count < 3")

    # Convert results to dataframe
    df = pd.DataFrame(posts_to_scrape, columns=["link"])

    # Create necessary directories
    os.makedirs('../data/testing_data', exist_ok=True)

    # Iterate through posts
    logger.info(f"Scraping Posts: {df['link'].to_list()}")
    for _, row in df.iterrows():
        try:
            # Log pre-scrape state
            utils.log_query_results(
                cursor,
                f"Pre-scrape state for {row['link']}",
                """
                SELECT p.post_url, p.scrape_count, p.total_reactions, 
                       (SELECT COUNT(*) FROM linkedin_engagers e WHERE e.post_url = p.post_url) as engager_count
                FROM linkedin_posts p
                WHERE p.post_url = %s
                """,
                (row['link'],)
            )

            # Scrape posts
            post_scrape = utils.scrape_post(row["link"])
            logger.info(f"Post Scraped: {row['link']}")

            # Create a safe filename using hash of the URL
            safe_filename = hashlib.md5(row["link"].encode()).hexdigest()
            csv_path = f"../data/testing_data/scrape_{safe_filename}.csv"
            
            # Temporarily save as csv
            post_scrape.to_csv(csv_path)
            logger.info(f"Saved scrape data to {csv_path}")

            # Ingest posts to PostgreSQL DB
            utils.ingest_scrape(post_scrape) 
            logger.info(f"Ingested to PostgreSQL DB: {row['link']}")

            # Log post-scrape state
            utils.log_query_results(
                cursor,
                f"Post-scrape state for {row['link']}",
                """
                SELECT p.post_url, p.scrape_count, p.total_reactions, 
                       (SELECT COUNT(*) FROM linkedin_engagers e WHERE e.post_url = p.post_url) as engager_count
                FROM linkedin_posts p
                WHERE p.post_url = %s
                """,
                (row['link'],)
            )
            
            # Add random wait time between scrapes (between 30 and 60 seconds)
            wait_time = random.uniform(30, 60)
            logger.info(f"Waiting {wait_time:.2f} seconds before next scrape...")
            time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error processing post {row['link']}: {str(e)}")
            # Add a longer wait time after an error (between 60 and 120 seconds)
            wait_time = random.uniform(60, 120)
            logger.info(f"Error occurred. Waiting {wait_time:.2f} seconds before next attempt...")
            time.sleep(wait_time)
            continue

    # Log final state
    utils.log_query_results(
        cursor,
        "Final scrape summary",
        """
        SELECT 
            COUNT(DISTINCT p.post_url) as total_posts,
            AVG(p.scrape_count) as avg_scrape_count,
            SUM(p.total_reactions) as total_reactions,
            COUNT(DISTINCT e.linkedin_url) as total_engagers
        FROM linkedin_posts p
        LEFT JOIN linkedin_engagers e ON p.post_url = e.post_url
        """
    )

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
