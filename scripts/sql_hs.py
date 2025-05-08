import sqlite3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/sql_hs_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Load Environment variables
    logger.info("Loading environment variables")
    load_dotenv()

    # Connect to SQLite database
    logger.info("Connecting to SQLite database")
    try:
        conn = sqlite3.connect('../data/post_scrapes.db')
        logger.info("Successfully connected to SQLite database")
    except Exception as e:
        logger.error(f"Failed to connect to SQLite database: {str(e)}")
        raise

    # Get distinct local engagers to upload to HubSpot
    local_engagers = pd.read_sql_query("""
    SELECT DISTINCT e.linkedin_url, e.name, e.headline, MIN(p.post_name) as post_name
    FROM engagers e
    JOIN posts p on e.post_url=p.post_url
    GROUP BY e.linkedin_url, e.name, e.headline
    """, conn)
    local_engagers["firstname"] = local_engagers["name"].apply(lambda x: str(x).split()[0] if pd.notnull(x) and str(x).strip() else "")
    local_engagers["lastname"] = local_engagers["name"].apply(lambda x: " ".join(str(x).split()[1:]) if pd.notnull(x) and len(str(x).split()) > 1 else "")
    logger.info(f"Got local engagers:\n{local_engagers}")

    # Pull remote engagers from HubSpot Organic Social list
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    list_number = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=hs_linkedin_url"
    properties=["hs_linkedin_url"]
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})
    logger.info(f"Got HubSpot Engagers (list #{list_number}):\n{hubspot_engagers}")

    # Merge and keep all local_engagers, adding columns from hubspot_engagers where linkedin_url matches
    merged = pd.merge(local_engagers, hubspot_engagers, on="linkedin_url", how="left", indicator=True)

    # Only keep those where there was no match in hubspot_engagers
    engagers_to_upload = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
    logger.info(f"Subset of engagers to upload:\n{engagers_to_upload}")
    
    # Map local property names to hubspot property names
    engagers_to_upload_properties = {
        'linkedin_url': 'hs_linkedin_url',
        'headline': 'phantombuster_linkedin_headline',
        'post_name': 'post_name',
        'firstname': 'firstname',
        'lastname': 'lastname'
    }

    # Upload new contacts to HubSpot
    logger.info(f"Starting push to Hubspot...")
    utils.hubspot_push_contacts_to_list(hs_api_key, engagers_to_upload, engagers_to_upload_properties)
    logger.info(f"...Completed push to hubspot\nScript run successfully")

if __name__ == "__main__":
    main()
