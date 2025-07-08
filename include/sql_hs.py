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
        logging.FileHandler(f'logs/sql_hs_{datetime.now().strftime("%Y%m%d")}.log'),
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
    hs_api_key = airflow_utils.get_required_env_var("HUBSPOT_API_KEY")
    list_id = airflow_utils.get_optional_env_var("HUBSPOT_LIST_ID", "246")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=hs_linkedin_url"
    properties=["hs_linkedin_url"]
    hubspot_engagers = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties).rename(columns={"hs_linkedin_url": "linkedin_url"})
    logger.info(f"Got HubSpot Engagers (list #{list_id}):\n{hubspot_engagers}")

    # Get list of LinkedIn URLs that are already in HubSpot
    existing_urls = set(hubspot_engagers['linkedin_url'].dropna().unique())
    
    # Filter out engagers that are already in HubSpot
    engagers_to_upload = local_engagers[~local_engagers['linkedin_url'].isin(existing_urls)]

    # Deduplicate by linkedin_url and any other url-related columns
    url_columns = [col for col in engagers_to_upload.columns if 'url' in col]
    engagers_to_upload = engagers_to_upload.drop_duplicates(subset=url_columns)
    logger.info(f"Deduplicated engagers to upload by columns: {url_columns}. Unique contacts: {len(engagers_to_upload)}")
    logger.info(f"Subset of engagers to upload:\n{engagers_to_upload}")
    
    # Only push /in/ profiles to HubSpot
    engagers_to_upload = engagers_to_upload[engagers_to_upload['linkedin_url'].str.contains('/in/')]
    logger.info(f"Filtered engagers to upload for HubSpot push (only /in/ profiles):\n{engagers_to_upload}")

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

    # --- New: Update company and jobtitle for existing contacts ---
    # Fetch all HubSpot contacts with hs_linkedin_url, company, jobtitle
    hubspot_contacts = utils.hubspot_fetch_all_contacts(
        hs_api_key, ["hs_linkedin_url", "company", "jobtitle"]
    )
    logger.info(f"Fetched {len(hubspot_contacts)} HubSpot contacts for update check.")

    # Fetch all local engagers with linkedin_url, company, title
    cursor.execute("""
        SELECT linkedin_url, company, title FROM linkedin_engagers
        WHERE linkedin_url IS NOT NULL AND linkedin_url != ''
    """)
    local_engagers = pd.DataFrame(cursor.fetchall(), columns=["linkedin_url", "company", "title"])
    logger.info(f"Fetched {len(local_engagers)} local engagers for update check.")

    # Merge on linkedin_url/hs_linkedin_url
    merged = pd.merge(
        hubspot_contacts,
        local_engagers,
        left_on="hs_linkedin_url",
        right_on="linkedin_url",
        how="inner",
        suffixes=("_hubspot", "_local")
    )
    logger.info(f"Found {len(merged)} matching contacts between HubSpot and local DB.")

    # For each match, update if company or jobtitle differs
    update_count = 0
    for _, row in merged.iterrows():
        contact_id = row["id"]
        local_company = row["company_local"]
        local_title = row["title"]
        hs_company = row.get("company_hubspot")
        hs_jobtitle = row.get("jobtitle")
        # Only update if different and local value is not null/empty
        needs_update = False
        update_fields = {}
        if pd.notnull(local_company) and local_company and local_company != hs_company:
            update_fields["company"] = local_company
            needs_update = True
        if pd.notnull(local_title) and local_title and local_title != hs_jobtitle:
            update_fields["jobtitle"] = local_title
            needs_update = True
        if needs_update:
            utils.hubspot_update_contact(hs_api_key, contact_id, update_fields.get("company"), update_fields.get("jobtitle"))
            update_count += 1
    logger.info(f"Updated {update_count} HubSpot contacts with new company/jobtitle info.")

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