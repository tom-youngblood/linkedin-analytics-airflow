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
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT", "5432"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
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

def scrape_posts_by_profile():
    """
    Scrape LinkedIn posts by profile using Apify, clean the resulting DataFrame, and prepare columns for RDS ingestion.
    Returns:
        None (saves cleaned DataFrame to variable in scope)
    """
    # Load Google Sheet to get the number of posts to scrape
    encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]
    gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))
    gc = gspread.service_account_from_dict(gspread_credentials)
    links_df = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])
    max_posts = len(links_df)
    logger.info(f"Will scrape up to {max_posts} posts to match Google Sheet.")

    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))

    links_df = links_df[0:5]
    all_items = []
    page_number = 1

    while True:
        # Prepare the Actor input
        run_input = {
            "username": os.environ.get("LINKEDIN_PROFILE_URL"),
            "page_number": page_number,
            "limit": 100,
        }

        # Run the Actor and wait for it to finish
        run = client.actor("LQQIXN9Othf8f7R5n").call(run_input=run_input)

        # Get items from this page
        page_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        logger.info(f"Retrieved {len(page_items)} items from page {page_number}")
        
        # If no items returned, we've reached the end
        if not page_items:
            logger.info("No more items found, ending pagination")
            break
            
        all_items.extend(page_items)

        # If we've reached or exceeded the max_posts, stop
        if len(all_items) >= max_posts:
            logger.info(f"Reached the max number of posts ({max_posts}), ending pagination")
            all_items = all_items[:max_posts]  # Trim to exact number if over
            break
        
        # If we got less than 100 items, we've reached the end
        if len(page_items) < 100:
            logger.info("Received less than 100 items, ending pagination")
            break
            
        page_number += 1

    logger.info(f"Total items collected: {len(all_items)}")
    df = pd.DataFrame(all_items)

    logger.info("Cleaning columns...")
    # Clean columns: Author
    df["author"] = df["author"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    df["author"] = df["author"].apply(lambda x: f"{x['first_name']} {x['last_name']}")

    # Clean columns: Comments
    df["comments"] = df["stats"].apply(
        lambda x: ast.literal_eval(x)["comments"] if isinstance(x, str) else x.get("comments")
    )

    # Clean columns: Reposts
    df["reposts"] = df["stats"].apply(
        lambda x: ast.literal_eval(x)["reposts"] if isinstance(x, str) else x.get("comments")
    )

    # Clean columns: Reshared Posts
    df["reshared_post"] = df["reshared_post"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
    )
    df["reshared_post_url"] = df["reshared_post"].apply(
        lambda x: x.get("url") if isinstance(x, dict) else None
    )
    df["reshared_post_total_reactions"] = df["reshared_post"].apply(
        lambda x: x.get("stats", {}).get("total_reactions") if isinstance(x, dict) else None
    )

    # Clean columns: Media
    df["media"] = df["media"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
    )
    df["media_type"] = df["media"].apply(
        lambda x: x.get("type") if isinstance(x, dict) else None
    )
    df["media_url"] = df["media"].apply(
        lambda x: x.get("url") if isinstance(x, dict) else None
    )

    # Clean columns: Article
    df["article"] = df["article"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
    )

    # Add article_url column
    df["article_url"] = df["article"].apply(
        lambda x: x.get("url") if isinstance(x, dict) else None
    )

    # Add title column
    df["article_title"] = df["article"].apply(
        lambda x: x.get("title") if isinstance(x, dict) else None
    )
    logger.log("All columns prepared for RDS Database")

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

def scrape_posts_by_profile():
    """
    Scrapes LinkedIn posts from a profile and processes them into a structured DataFrame.

    This function determines which posts to scrape by first querying the database
    for posts that have not been enriched (`enriched = FALSE`). The count of these
    posts is used as a limit for the profile scraper.

    Returns:
        pandas.DataFrame: A processed DataFrame containing the scraped LinkedIn posts.
    """
    logger.info("Fetching unenriched posts from database to determine scrape limit...")
    try:
        conn = get_db_connection()
        unenriched_posts_df = pd.read_sql_query("SELECT post_url FROM linkedin_posts WHERE enriched = FALSE", conn)
        conn.close()
        max_posts = len(unenriched_posts_df)
        logger.info(f"Found {max_posts} unenriched posts. This will be the scrape limit.")
        if max_posts == 0:
            logger.info("No unenriched posts to process. Exiting function.")
            return pd.DataFrame() # Return empty df
    except Exception as e:
        logger.error(f"Failed to fetch posts from database: {str(e)}")
        return pd.DataFrame() # Return empty df

    # Initialize the ApifyClient with your API token
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))

    all_items = []
    page_number = 1

    while True:
        # Prepare the Actor input
        run_input = {
            "username": os.environ.get("LINKEDIN_PROFILE_URL"),
            "page_number": page_number,
            "limit": 100,
        }

        try:
            # Run the Actor and wait for it to finish
            run = client.actor("LQQIXN9Othf8f7R5n").call(run_input=run_input)
            
            # Get items from this page
            page_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            
            # If no items returned, we've reached the end
            if not page_items:
                logger.info("Scraping complete - no more items found")
                break
                
            all_items.extend(page_items)

            # If we've reached or exceeded the max_posts, stop
            if len(all_items) >= max_posts:
                logger.info(f"Scraping complete - reached target of {max_posts} posts")
                all_items = all_items[:max_posts]  # Trim to exact number if over
                break
            
            # If we got less than 100 items, we've reached the end
            if len(page_items) < 100:
                logger.info("Scraping complete - last page processed")
                break
                
            page_number += 1
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            break

    logger.info(f"Processing {len(all_items)} posts")
    df = pd.DataFrame(all_items)
    if df.empty:
        logger.info("No posts scraped. Returning empty DataFrame.")
        return df
    try:
        # Clean columns: Author
        df["author"] = df["author"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        df["author"] = df["author"].apply(lambda x: f"{x['first_name']} {x['last_name']}")

        # Clean columns: Comments
        df["comments"] = df["stats"].apply(
            lambda x: ast.literal_eval(x)["comments"] if isinstance(x, str) else x.get("comments")
        )

        # Clean columns: Reposts
        df["reposts"] = df["stats"].apply(
            lambda x: ast.literal_eval(x)["reposts"] if isinstance(x, str) else x.get("reposts")
        )

        # Clean columns: Reshared Posts
        df["reshared_post"] = df["reshared_post"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
        )
        df["reshared_post_url"] = df["reshared_post"].apply(
            lambda x: x.get("url") if isinstance(x, dict) else None
        )
        df["reshared_post_total_reactions"] = df["reshared_post"].apply(
            lambda x: x.get("stats", {}).get("total_reactions") if isinstance(x, dict) else None
        )

        # Clean columns: Media
        df["media"] = df["media"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
        )
        df["media_type"] = df["media"].apply(
            lambda x: x.get("type") if isinstance(x, dict) else None
        )
        df["media_url"] = df["media"].apply(
            lambda x: x.get("url") if isinstance(x, dict) else None
        )

        # Clean columns: Article
        df["article"] = df["article"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x != "nan" else x
        )

        # Add article_url column
        df["article_url"] = df["article"].apply(
            lambda x: x.get("url") if isinstance(x, dict) else None
        )

        # Add title column
        df["article_title"] = df["article"].apply(
            lambda x: x.get("title") if isinstance(x, dict) else None
        )
        logger.info("Data processing complete")
    except Exception as e:
        logger.error(f"Error during data processing: {str(e)}")

    # Save csv to function_outputs
    df.to_csv("../data/function_outputs/scrape_posts_by_profile.csv", index=False)

    return df

def scrape_post_media_info(url):
    """
    Scrapes detailed media information from LinkedIn posts.

    This function retrieves and processes media-related information from LinkedIn posts,
    including details about videos, images, and other media content. It uses the Apify
    platform to scrape the data and processes it into a structured format.

    Args:
        links (str or list): Single URL or list of URLs of LinkedIn posts to scrape.

    Returns:
        pandas.DataFrame: A DataFrame containing the posts' media information with the following columns:
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
    client = ApifyClient(os.environ.get("APIFY_API_KEY"))

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

def prepare_media_enrichment_data(scrape_posts_by_profile_df):
    """
    Prepares enrichment data by processing media information for each post.

    Args:
        scrape_posts_by_profile_urls (pd.DataFrame): DataFrame containing post data with 'url' column

    Returns:
        pd.DataFrame: DataFrame containing enriched media information for all posts
    """
    if scrape_posts_by_profile_df.empty:
        logger.info("No posts to enrich media for. Returning empty DataFrame.")
        return pd.DataFrame(columns=["post_url", "media_type", "duration", "mime_type", "thumbnail", "video_url", "image_url"])
    # Initialize DataFrame with desired columns
    media_info_df = pd.DataFrame(columns=["post_url", "media_type", "duration", "mime_type", "thumbnail", "video_url", "image_url"])
    logger.info(f"Media info DataFrame created:\n{media_info_df}")
    logger.info(f"Head of media to process:\n{scrape_posts_by_profile_df.head()}")
    logger.info(f"Processing media info for {len(scrape_posts_by_profile_df)} posts")
    
    try:
        # Extract all URLs into a list
        urls = scrape_posts_by_profile_df['url'].tolist()
        logger.info(f"Extracted URLs:\n{urls}")
        
        # Process all URLs in one batch
        for url in urls:
            logger.info(f"Processing post: {url}...")
            single_enriched_post = scrape_post_media_info(url)
            logger.info(f"Processed post: {url}. Returned DF:\n{single_enriched_post}\nAppending to media_info_df.\nmedia_info_df:\n:{media_info_df}")
            media_info_df = pd.concat([media_info_df, single_enriched_post], ignore_index=True)
            logger.info(f"media_info_df after concat:\n{media_info_df}")    
        
    except Exception as e:
        logger.error(f"Error during media info processing: {str(e)}")
        
    logger.info("Media info processing complete")

    # Save csv to function_outputs
    media_info_df.to_csv("../data/function_outputs/prepare_media_enrichment_data.csv", index=False)

    return media_info_df

def finalize_enrichment_output(scrape_posts_by_profile_df, prepare_media_enrichment_data_df):
    """
    Merge the two DataFrames and select the final columns for database ingestion.
    """
    logger.info("Performing merge...")
   
    # Clean the URLs in scrape_posts_by_profile_df
    scrape_posts_by_profile_df['url_clean'] = scrape_posts_by_profile_df['url'].str.split('?').str[0]

    # Now merge on the cleaned URL
    merged = scrape_posts_by_profile_df.merge(
        prepare_media_enrichment_data_df,
        left_on='url_clean',
        right_on='post_url',
        how='left'
    )

    logger.info(f"Merge complete:\n{merged}")
    merged.to_csv("../data/function_outputs/finalized_enrichment_output.csv")
    return merged

def ingest_enriched_data_to_db(df):
    """
    Ingests the enriched data into the database, updating existing post records.

    For each row in the DataFrame, it finds the corresponding post in the
    'linkedin_posts' table via the URL and updates it with the new, enriched
    data fields. It also sets the 'enriched' flag to TRUE and records the
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

        # List of columns to update in the database
        columns_to_update = [
            "text", "post_type", "comments", "reposts", "reshared_post_url",
            "reshared_post_total_reactions", "media_type", "media_url",
            "article_url", "article_title", "duration", "mime_type",
            "thumbnail", "video_url", "image_url"
        ]

        for _, row in df.iterrows():
            post_url = row.get('url')
            if pd.isna(post_url):
                logger.warning("Skipping a row because its URL is missing.")
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
