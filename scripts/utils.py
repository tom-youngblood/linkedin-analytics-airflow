import sqlite3
import json
from datetime import datetime
import os
from apify_client import ApifyClient

def scrape_post(link):
    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ("APIFY_API_KEY"))
    
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
    
    return all_items

def ingest_scrape(conn, cursor, json_data):
    # Get post_url and total_reactions from the first item (all items have same post_url)
    post_url = json_data[0]['_metadata']['post_url']
    total_reactions = json_data[0]['_metadata']['total_reactions']
    ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Ingest scrapes data (single row)
    cursor.execute("""
        INSERT INTO scrapes (post_url, ran_at, reactions_count, cost, status)
        VALUES (?, ?, ?, ?, ?)
    """, (post_url, ran_at, total_reactions, 0, 'success'))
    scrape_id = cursor.lastrowid

    # Ingest engagers data (multiple rows: all engagers)
    for item in json_data:
        linkedin_url = item['reactor']['profile_url']
        engagement_type = item['reaction_type']
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