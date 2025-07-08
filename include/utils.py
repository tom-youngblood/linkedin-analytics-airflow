import ast
import base64
import json
import logging
import os
from datetime import datetime
import gspread
import pandas as pd
import psycopg2
from apify_client import ApifyClient
from dotenv import load_dotenv
from psycopg2.extras import DictCursor
import requests
import airflow_utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/utils.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
load_dotenv()

def log_query_results(cursor, query_name, query, params=None):
    """
    Execute a SQL query and log the results.
    Args:
        cursor: Database cursor
        query_name: Name of the query for logging
        query: SQL query to execute
        params: Query parameters (optional)
    Returns:
        List of query results
    """
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        logger.info(f"{query_name} results: {len(results)} rows")
        if results:
            logger.debug(f"Sample data: {results[0]}")
        return results
    except Exception as e:
        logger.error(f"Error executing {query_name}: {str(e)}")
        raise

def get_db_connection():
    """
    Create and return a connection to the PostgreSQL database using environment variables.
    Returns:
        psycopg2 connection object
    """
    try:
        conn = psycopg2.connect(
            host=airflow_utils.get_required_env_var("DB_HOST"),
            port=airflow_utils.get_optional_env_var("DB_PORT", "5432"),
            database=airflow_utils.get_required_env_var("DB_NAME"),
            user=airflow_utils.get_required_env_var("DB_USER"),
            password=airflow_utils.get_required_env_var("DB_PASSWORD"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def get_db_cursor(conn):
    """
    Create and return a DictCursor for the given PostgreSQL connection.
    Args:
        conn: psycopg2 connection object
    Returns:
        psycopg2 DictCursor object
    """
    return conn.cursor(cursor_factory=DictCursor)

def scrape_post_engagers(link):
    """
    Scrape LinkedIn post engagers using Apify and return a DataFrame of engagers.
    Args:
        link: LinkedIn post URL (str)
    Returns:
        DataFrame containing post engagers and their details
    """
    logger.info(f"Starting scrape for LinkedIn post: {link}")
    
    # Initialize the ApifyClient with your API token
    client = ApifyClient(airflow_utils.get_required_env_var("APIFY_API_KEY"))
    
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

def ingest_scrape(df):
    """
    Ingest scraped post and engagers data into the PostgreSQL database.
    Args:
        df: DataFrame from scrape_post function
    Returns:
        None
    """
    
    try:
        logger.info("Starting data ingestion process")
        # Get post_url and total_reactions from the first row (all rows have same post_url and total_reactions)
        post_url = df['metadata_post_url'].iloc[0]
        # Convert total_reactions to integer, removing any leading zeros
        total_reactions = int(df['metadata_total_reactions'].iloc[0])
        ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Processing post: {post_url} with {total_reactions} total reactions")

        conn = get_db_connection()
        cursor = get_db_cursor(conn)

        # First, check if post exists and get current scrape count
        cursor.execute("""
            SELECT scrape_count 
            FROM linkedin_posts 
            WHERE post_url = %s
        """, (post_url,))
        result = cursor.fetchone()
        
        if result is None:
            logger.info(f"Creating new post record for {post_url}")
            # Post doesn't exist, create it first with initial scrape count of 1
            cursor.execute("""
                INSERT INTO linkedin_posts (post_url, last_scraped_at, scrape_count, total_reactions)
                VALUES (%s, %s, 1, %s)
            """, (post_url, ran_at, total_reactions))
        else:
            current_count = result['scrape_count'] or 0  # Handle NULL case
            logger.info(f"Updating existing post. Current scrape count: {current_count}")
            # Update existing post with incremented scrape count
            cursor.execute("""
                UPDATE linkedin_posts
                SET last_scraped_at = %s,
                    scrape_count = %s,
                    total_reactions = %s
                WHERE post_url = %s
            """, (ran_at, current_count + 1, total_reactions, post_url))

        # Ingest scrapes data with current timestamp
        logger.info("Creating new scrape record")
        cursor.execute("""
            INSERT INTO linkedin_posts_scrapes (post_url, ran_at, reactions_count, cost, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (post_url, ran_at, total_reactions, 0, 'success'))
        scrape_id = cursor.fetchone()['id']
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
                INSERT INTO linkedin_engagers (scrape_id, linkedin_url, name, headline, engagement_type, post_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (linkedin_url, post_url) DO NOTHING
            """, (scrape_id, linkedin_url, name, headline, engagement_type, post_url))
            engager_count += 1

        logger.info(f"Processed {engager_count} engagers")

        # Commit the transaction
        conn.commit()
        logger.info("Successfully committed all changes to database")
        
        # Close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        # Rollback in case of error
        logger.error(f"Error during data ingestion: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise Exception(f"Error in ingest_scrape: {str(e)}")

def hubspot_fetch_list_contacts(api_key, url, properties):
    """
    Fetch a list of contacts from a HubSpot list and return as a DataFrame.
    Args:
        api_key: HubSpot API key (str)
        url: HubSpot list URL (str)
        properties: List of properties to retrieve (list)
    Returns:
        DataFrame of contacts
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
    """
    Push contacts from a DataFrame to HubSpot using a mapping of local column names to HubSpot property names.
    Args:
        api_key: HubSpot API key (str)
        df: DataFrame of contacts
        properties_map: Dict mapping local column names to HubSpot property names
    Returns:
        None
    """
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

def get_unenriched_posts_from_db():
    """
    Fetches unenriched posts from the database for individual processing.
    
    This function queries the database for posts that have not been enriched
    (`enriched = FALSE`) and returns them as a DataFrame with basic post information.
    This replaces the risky profile scraping approach with a safer individual post approach.
    
    Returns:
        pandas.DataFrame: A DataFrame containing unenriched posts with columns:
            - post_url: URL of the post
            - post_name: Name/description of the post
            - id: Database ID of the post
    """
    logger.info("Fetching unenriched posts from database for individual processing...")
    try:
        conn = get_db_connection()
        unenriched_posts_df = pd.read_sql_query(
            "SELECT post_url, post_name, id FROM linkedin_posts WHERE enriched = FALSE", 
            conn
        )
        conn.close()
        
        if unenriched_posts_df.empty:
            logger.info("No unenriched posts found in database.")
            return pd.DataFrame()
            
        logger.info(f"Found {len(unenriched_posts_df)} unenriched posts for processing.")
        return unenriched_posts_df
        
    except Exception as e:
        logger.error(f"Failed to fetch unenriched posts from database: {str(e)}")
        return pd.DataFrame()

def scrape_post_media_info(url):
    """
    Scrapes detailed media information from LinkedIn posts.

    This function retrieves and processes media-related information from LinkedIn posts,
    including details about videos, images, and other media content. It uses the Apify
    platform to scrape the data and processes it into a structured format.

    Args:
        url (str): URL of the LinkedIn post to scrape.

    Returns:
        pandas.DataFrame: A DataFrame containing the post's media information with the following columns:
            - post_url: URL of the post
            - media_type: Type of media ('video', 'image', etc.)
            For video content:
                - duration: Length of the video
                - mime_type: MIME type of the video
                - thumbnail: URL of the video thumbnail
                - video_url: URL of the video content
            For image content:
                - image_url: URL of the image
            Additional metadata about the post and its media content

    Notes:
        - The function handles both video and image content differently
        - For non-media posts or when media information is unavailable, relevant fields will be None
        - The function uses ast.literal_eval to safely convert string representations of dictionaries
    """
    # Initialize the ApifyClient with your API token
    client = ApifyClient(airflow_utils.get_required_env_var("APIFY_API_KEY"))

    # Prepare the Actor input
    run_input = {"post_url": url}

    # Run the Actor and wait for it to finish
    logger.info(f"Running actor for {url}")
    run = client.actor("d0DhjXPjkkwm4W5xK").call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    logger.info(f"Run complete: received {len(items)} items")

    if not items:
        logger.warning("No items returned from Apify")
        return pd.DataFrame()

    df = pd.DataFrame(items)
    logger.info("Performing transformations on DataFrame...")

    # Convert all columns to literal python dict
    for property in ["post", "media"]:
        df[property] = df[property].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
        )
    
    # Unpack post url
    df["post_url"] = df["post"].apply(lambda x: x.get("url") if isinstance(x, dict) and x != "nan" else x)

    # Robust media extraction
    def get_media_field(x, field, media_type=None):
        if isinstance(x, list) and x and isinstance(x[0], dict):
            if media_type is None or x[0].get("type") == media_type:
                return x[0].get(field)
        return None
    df["media_type"] = df["media"].apply(lambda x: get_media_field(x, "type"))
    df["duration"] = df["media"].apply(lambda x: get_media_field(x, "duration", "video"))
    df["mime_type"] = df["media"].apply(lambda x: get_media_field(x, "mime_type", "video"))
    df["thumbnail"] = df["media"].apply(lambda x: get_media_field(x, "thumbnail", "video"))
    df["video_url"] = df["media"].apply(lambda x: get_media_field(x, "video_url", "video"))
    df["image_url"] = df["media"].apply(lambda x: get_media_field(x, "url", "image"))

    return df

def prepare_media_enrichment_data(unenriched_posts_df):
    """
    Prepares enrichment data by processing media information for each post.

    Args:
        unenriched_posts_df (pd.DataFrame): DataFrame containing post data with 'post_url' column

    Returns:
        pd.DataFrame: DataFrame containing enriched media information for all posts
    """
    if unenriched_posts_df.empty:
        logger.info("No posts to enrich media for. Returning empty DataFrame.")
        return pd.DataFrame(columns=["post_url", "media_type", "duration", "mime_type", "thumbnail", "video_url", "image_url"])
    
    # Initialize DataFrame with desired columns
    media_info_df = pd.DataFrame(columns=["post_url", "media_type", "duration", "mime_type", "thumbnail", "video_url", "image_url"])
    logger.info(f"Media info DataFrame created with columns: {media_info_df.columns.tolist()}")
    logger.info(f"Processing media info for {len(unenriched_posts_df)} posts")
    
    try:
        # Extract all URLs into a list
        urls = unenriched_posts_df['post_url'].tolist()
        logger.info(f"Extracted {len(urls)} URLs for processing")
        
        # Process all URLs in one batch
        for url in urls:
            logger.info(f"Processing post: {url}...")
            single_enriched_post = scrape_post_media_info(url)
            if not single_enriched_post.empty:
                media_info_df = pd.concat([media_info_df, single_enriched_post], ignore_index=True)
                logger.info(f"Successfully processed post: {url}")
            else:
                logger.warning(f"No media data returned for post: {url}")
        
    except Exception as e:
        logger.error(f"Error during media info processing: {str(e)}")
        
    logger.info(f"Media info processing complete. Processed {len(media_info_df)} posts with media data.")
    return media_info_df

def finalize_enrichment_output(unenriched_posts_df, media_enrichment_df):
    """
    Merge the unenriched posts DataFrame with media enrichment data.
    
    Args:
        unenriched_posts_df (pd.DataFrame): DataFrame containing unenriched posts from database
        media_enrichment_df (pd.DataFrame): DataFrame containing media enrichment data
    
    Returns:
        pd.DataFrame: Merged DataFrame with all enrichment data
    """
    logger.info("Performing merge of unenriched posts with media enrichment data...")
   
    # Clean the URLs in unenriched_posts_df for matching
    unenriched_posts_df['url_clean'] = unenriched_posts_df['post_url'].str.split('?').str[0]

    # Now merge on the cleaned URL
    merged = unenriched_posts_df.merge(
        media_enrichment_df,
        left_on='url_clean',
        right_on='post_url',
        how='left'
    )

    logger.info(f"Merge complete. Final DataFrame has {len(merged)} rows.")
    return merged

def ingest_enriched_data_to_db(df):
    """
    Ingests the enriched media data into the database, updating existing post records.

    For each row in the DataFrame, it finds the corresponding post in the
    'linkedin_posts' table via the URL and updates it with the new, enriched
    media data fields. It also sets the 'enriched' flag to TRUE and records the
    'enriched_time'.

    Args:
        df (pd.DataFrame): The DataFrame containing the final merged and enriched data.
    """
    logger.info(f"Starting ingestion of {len(df)} enriched posts into the database.")
    
    conn = None  # Initialize conn to None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        update_count = 0
        error_count = 0
        no_match_count = 0

        # List of columns to update in the database (only media-related fields)
        columns_to_update = [
            "media_type", "duration", "mime_type", "thumbnail", "video_url", "image_url"
        ]

        for _, row in df.iterrows():
            post_url = row.get('post_url')  # Use post_url from the merged DataFrame
            if pd.isna(post_url):
                logger.warning("Skipping a row because its post_url is missing.")
                error_count += 1
                continue

            # Clean the URL by removing query parameters for matching
            clean_url = post_url.split('?')[0] if isinstance(post_url, str) else post_url

            # Build the SET clause and the values for the SQL query
            set_parts = []
            values = []
            for col in columns_to_update:
                value = row.get(col)
                # Ensure pandas NaN is converted to None for SQL NULL
                if pd.isna(value):
                    values.append(None)
                else:
                    values.append(value)
                set_parts.append(f"{col} = %s")
            
            # Add the 'enriched' flag to the update
            set_parts.append("enriched = TRUE")
            # Add 'enriched_time' to the update
            set_parts.append("enriched_time = %s")
            values.append(datetime.now())
            
            # Add the clean_url for the WHERE clause
            values.append(clean_url)

            # Construct the final UPDATE query using split_part to clean URLs in the database
            sql_query = f"""
                UPDATE linkedin_posts 
                SET {', '.join(set_parts)} 
                WHERE split_part(post_url, '?', 1) = %s
            """

            try:
                cursor.execute(sql_query, tuple(values))
                if cursor.rowcount == 0:
                    logger.warning(f"No matching post found for URL: {post_url} (cleaned: {clean_url})")
                    no_match_count += 1
                else:
                    update_count += 1
                    logger.info(f"Successfully updated post: {post_url} (cleaned: {clean_url})")
            except Exception as e:
                logger.error(f"Failed to update post {post_url}: {str(e)}")
                error_count += 1
        
        conn.commit()
        logger.info(f"Ingestion complete. Successfully updated: {update_count}, Errors: {error_count}, No matches: {no_match_count}")

    except Exception as e:
        logger.error(f"A critical error occurred during the database operation: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

def scrape_company(url):
    """
    Scrapes company and title from LinkedIn profile url.

    Args:
        url: linkedin profile-url

    Returns:
        dict: Dictionary containing of the format {profile_url: str, company: str, title: str}
    """
    # Initialize the ApifyClient with your API token
    client = ApifyClient(airflow_utils.get_required_env_var("APIFY_API_KEY"))

    # Prepare the Actor input
    run_input = {"username": url}

    # Run the Actor and wait for it to finish
    logger.info(f"Running actor for {url}")
    run = client.actor("VhxlqQXRwhW8H5hNV").call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    logger.info(f"Run complete: received {len(items)} items")

    # Extract current company and most recent job title
    if not items:
        logger.warning(f"No items returned from Apify for {url}")
        return {"profile_url": url, "company": None, "title": None}

    profile = items[0]
    company = None
    title = None

    # Extract current company from basic_info if available
    basic_info = profile.get("basic_info", {})
    company = basic_info.get("current_company")

    # Extract most recent job title from experience
    experience = profile.get("experience", [])
    if experience and isinstance(experience, list):
        # Find the most recent/current job (is_current True, or first in list)
        current_jobs = [exp for exp in experience if exp.get("is_current")]
        if current_jobs:
            # If there are current jobs, pick the first one
            title = current_jobs[0].get("title")
            if not company:
                company = current_jobs[0].get("company")
        else:
            # Otherwise, pick the most recent job (first in list)
            title = experience[0].get("title")
            if not company:
                company = experience[0].get("company")

    return {"profile_url": url, "company": company, "title": title} 