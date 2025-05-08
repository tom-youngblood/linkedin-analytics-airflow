import sqlite3
import json
from datetime import datetime
import os
from apify_client import ApifyClient
import pandas as pd

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
    
    # Get post_url and total_reactions from the first row (all rows have same post_url and total_reactions)
    post_url = df['metadata_post_url'].iloc[0]
    total_reactions = df['metadata_total_reactions'].iloc[0]
    ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
        cursor.execute("""
            INSERT INTO engagers (scrape_id, linkedin_url, engagement_type)
            VALUES (?, ?, ?)
        """, (scrape_id, linkedin_url, engagement_type))

    # Ingest posts data (single row)
    cursor.execute("""
        UPDATE posts
        SET last_scraped_at = ?, scrape_count = COALESCE(scrape_count, 0) + 1, total_reactions = ?
        WHERE post_url = ?
    """, (ran_at, total_reactions, post_url))

    return conn, cursor