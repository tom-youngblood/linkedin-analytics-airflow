from apify_client import ApifyClient
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import gspread
import json
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/testing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Scrape scrape_posts_by_profile
# Later, this will need to be changed to get only the number of posts that have not been categorized yet using RDS (for efficiency)
def scrape_posts_by_profile():
    # Load Google Sheet to get the number of posts to scrape
    encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
    gc = gspread.service_account_from_dict(gspread_credentials)
    links_df = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])
    max_posts = len(links_df)
    logger.info(f"Will scrape up to {max_posts} posts to match Google Sheet.")

    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))

    links_df = links_df[0:5]
    all_items = []
    page_number = 1

    while True:
        # Prepare the Actor input
        run_input = {
            "username": os.environ.get("LINKEDIN_PROFILE_URL"),
            "page_number": page_number,
            "limit": 100,
        }

        # Run the Actor and wait for it to finish
        run = client.actor("LQQIXN9Othf8f7R5n").call(run_input=run_input)

        # Get items from this page
        page_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        logger.info(f"Retrieved {len(page_items)} items from page {page_number}")
        
        # If no items returned, we've reached the end
        if not page_items:
            logger.info("No more items found, ending pagination")
            break
            
        all_items.extend(page_items)

        # If we've reached or exceeded the max_posts, stop
        if len(all_items) >= max_posts:
            logger.info(f"Reached the max number of posts ({max_posts}), ending pagination")
            all_items = all_items[:max_posts]  # Trim to exact number if over
            break
        
        # If we got less than 100 items, we've reached the end
        if len(page_items) < 100:
            logger.info("Received less than 100 items, ending pagination")
            break
            
        page_number += 1

    logger.info(f"Total items collected: {len(all_items)}")
    df = pd.DataFrame(all_items)
    df.to_csv("temp.csv")


# Test scrape posts by profile
scrape_posts_by_profile()