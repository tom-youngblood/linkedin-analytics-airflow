"""
This script performs a one-time data migration from the local RDS database to HubSpot.
It is intended to be run once to transfer existing enriched contact data before the
pipeline is overhauled to make HubSpot the single source of truth for contact properties.

**Prerequisites:**
1. Ensure the required environment variables (DB credentials, HubSpot API key) are set.
2. Manually create the following custom Contact Properties in HubSpot:
   - `engager_audience` (Type: Single-line text)
   - `engager_bucketed_position` (Type: Single-line text)

**What it does:**
1. Fetches all unique, enriched engagers from the local `linkedin_engagers` table.
2. Fetches existing contacts from HubSpot List 246.
3. Compares the data from RDS with the data in HubSpot for each matching contact.
4. Updates HubSpot contacts with any new or different information from the RDS database.
"""

import logging
import pandas as pd
import requests
import utils
import airflow_utils
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/one_time_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def update_hubspot_contact_properties(api_key, contact_id, properties_dict):
    """
    Updates a HubSpot contact with a dictionary of properties.

    Args:
        api_key (str): The HubSpot API key.
        contact_id (str): The ID of the contact to update.
        properties_dict (dict): A dictionary of properties to update.
    
    Returns:
        bool: True if the update was successful, False otherwise.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    data = {"properties": properties_dict}
    
    try:
        response = requests.patch(url, headers=headers, json=data)
        if response.status_code == 200:
            logger.info(f"Successfully updated contact {contact_id} with properties: {properties_dict.keys()}")
            return True
        else:
            logger.error(f"Failed to update contact {contact_id}: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"An exception occurred while updating contact {contact_id}: {e}")
        return False

def main():
    logger.info("Starting one-time data migration from RDS to HubSpot...")
    load_dotenv()

    # --- 1. Fetch Data from RDS ---
    logger.info("Fetching enriched data from local RDS database...")
    try:
        conn = utils.get_db_connection()
        cursor = utils.get_db_cursor(conn)
        
        rds_query = """
            SELECT DISTINCT
                linkedin_url,
                company,
                title,
                engager_audience,
                engager_bucketed_position
            FROM linkedin_engagers
            WHERE 
                linkedin_url IS NOT NULL AND linkedin_url != ''
                AND (company IS NOT NULL OR title IS NOT NULL OR engager_audience IS NOT NULL)
        """
        rds_engagers = utils.log_query_results(cursor, "RDS Enriched Engagers", rds_query)
        rds_df = pd.DataFrame(rds_engagers, columns=["linkedin_url", "company", "title", "engager_audience", "engager_bucketed_position"])
        logger.info(f"Found {len(rds_df)} unique, enriched engagers in RDS.")
        logger.info(f"{rds_df.head()}")
        
    except Exception as e:
        logger.error(f"Failed to fetch data from RDS: {e}")
        return
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

    if rds_df.empty:
        logger.info("No enriched data found in RDS. Migration not needed.")
        return

    # --- 2. Fetch Data from HubSpot List 246 ---
    logger.info("Fetching contacts from HubSpot List 246...")
    try:
        hs_api_key = airflow_utils.get_required_env_var("HUBSPOT_API_KEY")
        list_id = airflow_utils.get_optional_env_var("HUBSPOT_LIST_ID", "246")
        url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all"
        
        # Properties to fetch, including the new custom ones
        hs_properties = ["hs_linkedin_url", "company", "jobtitle", "engager_audience", "engager_bucketed_position"]
        hubspot_contacts_df = utils.hubspot_fetch_list_contacts(hs_api_key, url, hs_properties)
        logger.info(f"Found {len(hubspot_contacts_df)} contacts in HubSpot list {list_id}.")
        logger.info(f"{hubspot_contacts_df.head()}")
    except Exception as e:
        logger.error(f"Failed to fetch data from HubSpot: {e}")
        return

    if hubspot_contacts_df.empty:
        logger.warning("No contacts found in HubSpot list. Cannot perform updates.")
        return

    # --- 3. Merge Data ---
    logger.info("Merging RDS and HubSpot data...")
    # Clean URLs for a reliable join
    rds_df['linkedin_url_clean'] = rds_df['linkedin_url'].str.strip().str.lower()
    hubspot_contacts_df['hs_linkedin_url_clean'] = hubspot_contacts_df['hs_linkedin_url'].str.strip().str.lower()

    merged_df = pd.merge(
        hubspot_contacts_df,
        rds_df,
        left_on="hs_linkedin_url_clean",
        right_on="linkedin_url_clean",
        how="inner",
        suffixes=("_hs", "_rds")
    )
    logger.info(f"Found {len(merged_df)} matching contacts between HubSpot and RDS.")

    # --- 4. Compare and Update HubSpot ---
    update_count = 0
    for _, row in merged_df.iterrows():
        contact_id = row.get("vid") # 'vid' is the contact ID from the v1 API
        if not contact_id:
            continue

        properties_to_update = {}

        # Compare Company
        if pd.notnull(row['company_rds']) and row['company_rds'] != row['company_hs']:
            properties_to_update['company'] = row['company_rds']
        
        # Compare Title/Jobtitle
        if pd.notnull(row['title']) and row['title'] != row['jobtitle']:
            properties_to_update['jobtitle'] = row['title']
            
        # Compare Engager Audience
        if pd.notnull(row['engager_audience_rds']) and row['engager_audience_rds'] != row['engager_audience_hs']:
            properties_to_update['engager_audience'] = row['engager_audience_rds']
            
        # Compare Bucketed Position
        if pd.notnull(row['engager_bucketed_position_rds']) and row['engager_bucketed_position_rds'] != row['engager_bucketed_position_hs']:
            properties_to_update['engager_bucketed_position'] = row['engager_bucketed_position_rds']

        if properties_to_update:
            if update_hubspot_contact_properties(hs_api_key, contact_id, properties_to_update):
                update_count += 1
    
    logger.info("--- Migration Summary ---")
    logger.info(f"Total contacts matched: {len(merged_df)}")
    logger.info(f"Total contacts updated in HubSpot: {update_count}")
    logger.info("One-time migration script finished.")

if __name__ == "__main__":
    main()
