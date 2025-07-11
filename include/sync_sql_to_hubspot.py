import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import utils
import airflow_utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/sync_sql_to_hubspot_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Load Environment variables (for backward compatibility)
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

    # 1. Get all local engagers with their full details in one query
    local_engagers_query = """
        SELECT
            e.linkedin_url,
            e.name,
            e.headline,
            MIN(p.post_name) as post_name
        FROM linkedin_engagers_by_post e
        JOIN linkedin_posts p ON e.post_url = p.post_url
        WHERE e.linkedin_url IS NOT NULL AND e.linkedin_url != ''
        GROUP BY e.linkedin_url, e.name, e.headline
    """
    local_engagers = utils.log_query_results(cursor, "Local engagers query", local_engagers_query)
    local_engagers_df = pd.DataFrame(local_engagers, columns=["linkedin_url", "name", "headline", "post_name"])
    local_engagers_df["firstname"] = local_engagers_df["name"].apply(lambda x: str(x).split()[0] if pd.notnull(x) and str(x).strip() else "")
    local_engagers_df["lastname"] = local_engagers_df["name"].apply(lambda x: " ".join(str(x).split()[1:]) if pd.notnull(x) and len(str(x).split()) > 1 else "")
    logger.info(f"Got {len(local_engagers_df)} total local engagers. Head:")
    logger.info(f"{local_engagers_df.head()}")

    # 2. Fetch HubSpot contacts ONCE with all required properties
    hs_api_key = airflow_utils.get_required_env_var("HUBSPOT_API_KEY")
    list_id = airflow_utils.get_optional_env_var("HUBSPOT_LIST_ID", "246")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all"
    properties = ["hs_linkedin_url", "company", "jobtitle", "engager_audience", "engager_bucketed_position"]
    hubspot_contacts_df = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties)
    logger.info(f"Fetched {len(hubspot_contacts_df)} HubSpot contacts from list {list_id}. Head:")
    logger.info(f"{hubspot_contacts_df.head()}")

    # Clean URLs for matching
    local_engagers_df['linkedin_url_clean'] = local_engagers_df['linkedin_url'].str.strip().str.lower()
    hubspot_contacts_df['hs_linkedin_url_clean'] = hubspot_contacts_df['hs_linkedin_url'].str.strip().str.lower()

    # 3. Identify new contacts to upload
    existing_urls = set(hubspot_contacts_df['hs_linkedin_url_clean'].dropna().unique())
    engagers_to_upload = local_engagers_df[~local_engagers_df['linkedin_url_clean'].isin(existing_urls)]
    engagers_to_upload = engagers_to_upload[engagers_to_upload['linkedin_url'].str.contains('/in/', na=False)]
    logger.info(f"Found {len(engagers_to_upload)} new contacts to upload to HubSpot. Head:")
    logger.info(f"{engagers_to_upload.head()}")

    # Upload new contacts to HubSpot
    if not engagers_to_upload.empty:
        engagers_to_upload_properties = {
            'linkedin_url': 'hs_linkedin_url',
            'headline': 'phantombuster_linkedin_headline',
            'post_name': 'post_name',
            'firstname': 'firstname',
            'lastname': 'lastname'
        }
        logger.info(f"Starting push of {len(engagers_to_upload)} new contacts to HubSpot...")
        utils.hubspot_push_contacts_to_list(hs_api_key, engagers_to_upload, engagers_to_upload_properties)
        logger.info("...Completed push to HubSpot.")
    else:
        logger.info("No new contacts to upload.")

    # Log final statistics
    utils.log_query_results(
        cursor,
        "Post-HubSpot sync statistics",
        """
        SELECT
            COUNT(DISTINCT e.linkedin_url) as total_engagers,
            COUNT(DISTINCT CASE WHEN e.pushed_to_hubspot THEN e.linkedin_url END) as pushed_to_hubspot_count,
            COUNT(DISTINCT e.post_url) as total_posts_with_engagers
        FROM linkedin_engagers_by_post e
        """
    )

    cursor.close()
    conn.close()
    logger.info("SQL to HubSpot sync completed successfully.")

if __name__ == "__main__":
    main() 