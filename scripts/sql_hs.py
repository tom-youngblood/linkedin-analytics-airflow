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

    # Connect to PostgreSQL database
    logger.info("Connecting to PostgreSQL database")
    try:
        conn = utils.get_db_connection()
        cursor = utils.get_db_cursor(conn)
        logger.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
        raise

    # Get distinct local engagers to upload to HubSpot
    local_engagers = utils.log_query_results(
        cursor,
        "Local engagers query",
        """
        SELECT DISTINCT e.linkedin_url, e.name, e.headline, MIN(p.post_name) as post_name
        FROM linkedin_engagers e
        JOIN linkedin_posts p on e.post_url=p.post_url
        GROUP BY e.linkedin_url, e.name, e.headline
        """
    )
    local_engagers = pd.DataFrame(local_engagers, columns=["linkedin_url", "name", "headline", "post_name"])
    local_engagers["firstname"] = local_engagers["name"].apply(lambda x: str(x).split()[0] if pd.notnull(x) and str(x).strip() else "")
    local_engagers["lastname"] = local_engagers["name"].apply(lambda x: " ".join(str(x).split()[1:]) if pd.notnull(x) and len(str(x).split()) > 1 else "")
    logger.info(f"Got local engagers:\n{local_engagers}")

    # Log engager statistics
    utils.log_query_results(
        cursor,
        "Engager statistics",
        """
        SELECT 
            COUNT(DISTINCT e.linkedin_url) as total_engagers,
            COUNT(DISTINCT e.post_url) as total_posts_with_engagers,
            COUNT(DISTINCT CASE WHEN e.pushed_to_hubspot THEN e.linkedin_url END) as pushed_to_hubspot_count
        FROM linkedin_engagers e
        """
    )

    # Pull remote engagers from HubSpot Organic Social list
    hs_api_key = os.environ["HUBSPOT_API_KEY"]
    list_number = 246
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_number}/contacts/all?property=hs_linkedin_url"
    properties=["hs_linkedin_url"]
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})
    logger.info(f"Got HubSpot Engagers (list #{list_number}):\n{hubspot_engagers}")

    # Get list of LinkedIn URLs that are already in HubSpot
    existing_urls = set(hubspot_engagers['linkedin_url'].dropna().unique())
    
    # Filter out engagers that are already in HubSpot
    engagers_to_upload = local_engagers[~local_engagers['linkedin_url'].isin(existing_urls)]
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
    logger.info(f"...Completed push to hubspot")

    # Log final state after HubSpot sync
    utils.log_query_results(
        cursor,
        "Post-HubSpot sync statistics",
        """
        SELECT 
            COUNT(DISTINCT e.linkedin_url) as total_engagers,
            COUNT(DISTINCT CASE WHEN e.pushed_to_hubspot THEN e.linkedin_url END) as pushed_to_hubspot_count,
            COUNT(DISTINCT e.post_url) as total_posts_with_engagers
        FROM linkedin_engagers e
        """
    )

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
