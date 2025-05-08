import sqlite3
import json
import requests
from datetime import datetime
import os
from apify_client import ApifyClient
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def scrape_post(link):
    """Input: link (str) -- LinkedIn Link. Returns: final_df (df) -- DataFrame containing LinkedIn post engagers."""
    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))
    
    all_items = []
    page_number = 1
    
    while True:
        # Prepare the Actor input
        run_input = {
            "post_url": link,
            "page_number": page_number,
            "reaction_type": "ALL",
            "limit": 100,  # Maximum allowed per page
        }

        # Run the Actor and wait for it to finish
        run = client.actor("J9UfswnR3Kae4O6vm").call(run_input=run_input)
        
        # Get items from this page
        page_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        # If no items returned, we've reached the end
        if not page_items:
            break
            
        all_items.extend(page_items)
        
        # If we got less than 100 items, we've reached the end
        if len(page_items) < 100:
            break
            
        page_number += 1
    
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(all_items)
    
    # Flatten the nested JSON structure
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
    
    return final_df

def ingest_scrape(conn, cursor, df):
    """Input: conn (sqlite3.Connection), cursor (sqlite3.Cursor), df (pd.DataFrame) -- DataFrame from scrape_post function.
    Returns: conn, cursor -- Updated connection and cursor objects."""
    
    try:
        # Get post_url and total_reactions from the first row (all rows have same post_url and total_reactions)
        post_url = df['metadata_post_url'].iloc[0]
        # Convert total_reactions to integer, removing any leading zeros
        total_reactions = int(df['metadata_total_reactions'].iloc[0])
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # First, check if post exists and get current scrape count
        cursor.execute("""
            SELECT scrape_count 
            FROM posts 
            WHERE post_url = ?
        """, (post_url,))
        result = cursor.fetchone()
        
        if result is None:
            # Post doesn't exist, create it first
            cursor.execute("""
                INSERT INTO posts (post_url, last_scraped_at, scrape_count, total_reactions)
                VALUES (?, ?, 1, ?)
            """, (post_url, ran_at, total_reactions))
        else:
            current_count = result[0] or 0  # Handle NULL case
            # Update existing post
            cursor.execute("""
                UPDATE posts
                SET last_scraped_at = ?,
                    scrape_count = ?,
                    total_reactions = ?
                WHERE post_url = ?
            """, (ran_at, current_count + 1, total_reactions, post_url))

        # Ingest scrapes data (single row)
        cursor.execute("""
            INSERT INTO scrapes (post_url, ran_at, reactions_count, cost, status)
            VALUES (?, ?, ?, ?, ?)
        """, (post_url, ran_at, total_reactions, 0, 'success'))
        scrape_id = cursor.lastrowid

        # Ingest engagers data (multiple rows: all engagers)
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

        return conn, cursor

    except Exception as e:
        # Rollback in case of error
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
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        data = response.json()

        # Extract contacts and add to the list
        if "contacts" in data:
            contacts.extend(data["contacts"])

        # Check for pagination (if there are more contacts to fetch)
        if "vid-offset" in data and data.get("has-more", False):
            params["vidOffset"] = data["vid-offset"] 
        else:
            break

    contacts_list = []

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

    return pd.DataFrame(contacts_list)

def hubspot_push_contacts_to_list(api_key, df, properties_map):
    """Pushes all contacts from a DataFrame to HubSpot using a mapping of local column names to HubSpot property names."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    if len(df) == 0:
        logger.info("No new leads pushed")
        return

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

        response = requests.post(url, headers=headers, json=hubspot_record)

        if response.status_code == 201:
            logger.info(f"Successfully pushed: {row.get('name', row.get('firstname', ''))}")
        else:
            logger.error(f"Failed to push: {row.get('name', row.get('firstname', ''))}, Error: {response.text}")

    return None