import sqlite3
import json
import requests
from datetime import datetime
import os
from apify_client import ApifyClient
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scripts/logs/utils.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def scrape_post(link):
    """Input: link (str) -- LinkedIn Link. Returns: final_df (df) -- DataFrame containing LinkedIn post engagers."""
    logger.info(f"Starting scrape for LinkedIn post: {link}")
    
    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))
    
    all_items = []
    page_number = 1
    
    while True:
        logger.info(f"Fetching page {page_number} of reactions")
        # Prepare the Actor input
        run_input = {
            "post_url": link,
            "page_number": page_number,
            "reaction_type": "ALL",
            "limit": 100,  # Maximum allowed per page
        }

        # Run the Actor and wait for it to finish
        run = client.actor("J9UfswnR3Kae4O6vm").call(run_input=run_input)
        logger.info(f"Apify actor run completed with ID: {run['id']}")
        
        # Get items from this page
        page_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        logger.info(f"Retrieved {len(page_items)} items from page {page_number}")
        
        # If no items returned, we've reached the end
        if not page_items:
            logger.info("No more items found, ending pagination")
            break
            
        all_items.extend(page_items)
        
        # If we got less than 100 items, we've reached the end
        if len(page_items) < 100:
            logger.info("Received less than 100 items, ending pagination")
            break
            
        page_number += 1
    
    logger.info(f"Total items collected: {len(all_items)}")
    
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_items)
    
    # Flatten the nested JSON structure
    logger.info("Flattening nested JSON structure")
    # First, expand the reactor column
    reactor_df = pd.json_normalize(df['reactor'])
    # Rename columns to avoid conflicts
    reactor_df.columns = [f'reactor_{col}' for col in reactor_df.columns]
    
    # Expand the profile_pictures column
    profile_pics_df = pd.json_normalize(df['reactor'].apply(lambda x: x['profile_pictures']))
    profile_pics_df.columns = [f'profile_pic_{col}' for col in profile_pics_df.columns]
    
    # Expand the metadata column
    metadata_df = pd.json_normalize(df['_metadata'])
    metadata_df.columns = [f'metadata_{col}' for col in metadata_df.columns]
    
    # Combine all the expanded columns
    final_df = pd.concat([
        df['reaction_type'],
        reactor_df,
        profile_pics_df,
        metadata_df
    ], axis=1)
    
    logger.info(f"Successfully processed data into DataFrame with {len(final_df)} rows")
    return final_df

def ingest_scrape(conn, cursor, df):
    """Input: conn (sqlite3.Connection), cursor (sqlite3.Cursor), df (pd.DataFrame) -- DataFrame from scrape_post function.
    Returns: conn, cursor -- Updated connection and cursor objects."""
    
    try:
        logger.info("Starting data ingestion process")
        # Get post_url and total_reactions from the first row (all rows have same post_url and total_reactions)
        post_url = df['metadata_post_url'].iloc[0]
        # Convert total_reactions to integer, removing any leading zeros
        total_reactions = int(df['metadata_total_reactions'].iloc[0])
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Processing post: {post_url} with {total_reactions} total reactions")

        # First, check if post exists and get current scrape count
        cursor.execute("""
            SELECT scrape_count 
            FROM posts 
            WHERE post_url = ?
        """, (post_url,))
        result = cursor.fetchone()
        
        if result is None:
            logger.info(f"Creating new post record for {post_url}")
            # Post doesn't exist, create it first with initial scrape count of 1
            cursor.execute("""
                INSERT INTO posts (post_url, last_scraped_at, scrape_count, total_reactions)
                VALUES (?, ?, 1, ?)
            """, (post_url, ran_at, total_reactions))
        else:
            current_count = result[0] or 0  # Handle NULL case
            logger.info(f"Updating existing post. Current scrape count: {current_count}")
            # Update existing post with incremented scrape count
            cursor.execute("""
                UPDATE posts
                SET last_scraped_at = ?,
                    scrape_count = ?,
                    total_reactions = ?
                WHERE post_url = ?
            """, (ran_at, current_count + 1, total_reactions, post_url))

        # Ingest scrapes data with current timestamp
        logger.info("Creating new scrape record")
        cursor.execute("""
            INSERT INTO scrapes (post_url, ran_at, reactions_count, cost, status)
            VALUES (?, ?, ?, ?, ?)
        """, (post_url, ran_at, total_reactions, 0, 'success'))
        scrape_id = cursor.lastrowid
        logger.info(f"Created scrape record with ID: {scrape_id}")

        # Ingest engagers data (multiple rows: all engagers)
        logger.info("Processing engagers data")
        engager_count = 0
        # Using DataFrame's itertuples for better performance
        for row in df.itertuples():
            linkedin_url = row.reactor_profile_url
            engagement_type = row.reaction_type
            name = row.reactor_name
            headline = row.reactor_headline
            cursor.execute("""
                INSERT OR IGNORE INTO engagers (scrape_id, linkedin_url, name, headline, engagement_type, post_url)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (scrape_id, linkedin_url, name, headline, engagement_type, post_url))
            engager_count += 1

        logger.info(f"Processed {engager_count} engagers")

        # Commit the transaction
        conn.commit()
        logger.info("Successfully committed all changes to database")
        return conn, cursor

    except Exception as e:
        # Rollback in case of error
        logger.error(f"Error during data ingestion: {str(e)}")
        conn.rollback()
        raise Exception(f"Error in ingest_scrape: {str(e)}")

def hubspot_fetch_list_contacts(api_key, url, properties):
    """
    Inputs: 
    Hubspot API Key (STR),
    URL (Str): URL of list 
    Properties (list): All properties to be returned
    
    Returns: 
    DF: list of contacts.
    """
    logger.info(f"Fetching contacts from HubSpot list: {url}")
    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    contacts = []
    params = {
        "count": 100
    }

    while True:
        logger.info(f"Fetching batch of contacts with offset: {params.get('vidOffset', 'initial')}")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error(f"Error fetching contacts: {response.status_code}, {response.text}")
            break

        data = response.json()

        # Extract contacts and add to the list
        if "contacts" in data:
            batch_contacts = data["contacts"]
            contacts.extend(batch_contacts)
            logger.info(f"Retrieved {len(batch_contacts)} contacts in this batch")

        # Check for pagination (if there are more contacts to fetch)
        if "vid-offset" in data and data.get("has-more", False):
            params["vidOffset"] = data["vid-offset"]
            logger.info("More contacts available, continuing pagination")
        else:
            logger.info("No more contacts to fetch")
            break

    contacts_list = []
    logger.info(f"Processing {len(contacts)} total contacts")

    for contact in contacts:
        parsed_data = {"vid": contact.get("vid")}  # Extract vid

        # Extract all requested properties, setting None if missing
        if "properties" in contact:
            for prop in properties:
                if prop in contact["properties"]:
                    parsed_data[prop] = contact["properties"][prop].get("value")
                else:
                    parsed_data[prop] = None  # Ensures consistency across all rows

        contacts_list.append(parsed_data)  # Add to list

    logger.info(f"Successfully processed {len(contacts_list)} contacts into DataFrame")
    return pd.DataFrame(contacts_list)

def hubspot_push_contacts_to_list(api_key, df, properties_map):
    """Pushes all contacts from a DataFrame to HubSpot using a mapping of local column names to HubSpot property names."""
    logger.info(f"Starting HubSpot contact push for {len(df)} contacts")
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    if len(df) == 0:
        logger.info("No new leads to push")
        return

    success_count = 0
    error_count = 0

    for _, row in df.iterrows():
        # Build the properties dict using the mapping
        hubspot_properties = {}
        for local_col, hs_col in properties_map.items():
            value = row.get(local_col, "")
            # Convert NaN to empty string
            if pd.isnull(value):
                value = ""
            hubspot_properties[hs_col] = value

        hubspot_record = {"properties": hubspot_properties}
        contact_name = row.get('name', row.get('firstname', 'Unknown'))

        try:
            response = requests.post(url, headers=headers, json=hubspot_record)

            if response.status_code == 201:
                logger.info(f"Successfully pushed contact: {contact_name}")
                success_count += 1
            else:
                logger.error(f"Failed to push contact {contact_name}. Status: {response.status_code}, Error: {response.text}")
                error_count += 1
        except Exception as e:
            logger.error(f"Exception while pushing contact {contact_name}: {str(e)}")
            error_count += 1

    logger.info(f"HubSpot push completed. Successes: {success_count}, Errors: {error_count}")
    return None