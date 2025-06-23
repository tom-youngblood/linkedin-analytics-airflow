from apify_client import ApifyClient
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import gspread
import json
import base64
import ast
import utils
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/testing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Later, this will need to be changed to get only the number of posts that have not been categorized yet using RDS (for efficiency)
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
        conn = utils.get_db_connection()
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

    # Extract media type and video-specific properties
    df["media_type"] = df["media"].apply(lambda x: x[0].get("type") if x != "nan" else x)

    # Extract video-specific properties only when media type is video
    df["duration"] = df["media"].apply(lambda x: x[0].get("duration") if x != "nan" and x[0].get("type") == "video" else None)
    df["mime_type"] = df["media"].apply(lambda x: x[0].get("mime_type") if x != "nan" and x[0].get("type") == "video" else None)
    df["thumbnail"] = df["media"].apply(lambda x: x[0].get("thumbnail") if x != "nan" and x[0].get("type") == "video" else None)
    df["video_url"] = df["media"].apply(lambda x: x[0].get("video_url") if x != "nan" and x[0].get("type") == "video" else None)

    # Extract image-specific properties only when media type is image
    df["image_url"] = df["media"].apply(lambda x: x[0].get("url") if x != "nan" and x[0].get("type") == "image" else None)

    return df

def prepare_media_enrichment_data(scrape_posts_by_profile_df):
    """
    Prepares enrichment data by processing media information for each post.

    Args:
        scrape_posts_by_profile_urls (pd.DataFrame): DataFrame containing post data with 'url' column

    Returns:
        pd.DataFrame: DataFrame containing enriched media information for all posts
    """
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

    return media_info_df

def finalize_enrichment_output(scrape_posts_by_profile_df, prepare_media_enrichment_data_df):
    logger.info("Performing merge...")
    # Merge both DataFrames
    merged = pd.merge(left=scrape_posts_by_profile_df, 
                  right=prepare_media_enrichment_data_df,
                  left_on="url",
                  right_on="post_url",
                  how="outer")
    
    # Subset columns
    merged = merged[["url",
                 "text",
                 "post_type",
                 "comments",
                 "reposts",
                 "reshared_post_url",
                 "reshared_post_total_reactions",
                 "media_type_x",
                 "media_url",
                 "article_url",
                 "article_title",
                 "duration",
                 "mime_type",
                 "thumbnail",
                 "video_url",
                 "image_url",
                 "post"]].rename(columns={"media_type_x":"media_type"})

    logger.info("Merge complete")
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
        conn = utils.get_db_connection()
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

if __name__=="__main__":
    # Test the ingestion function with the final output file
    try:
        final_output_df = pd.read_csv("final_output.csv")
        print("\nStarting ingestion of enriched data into the database...")
        ingest_enriched_data_to_db(final_output_df)
        print("Ingestion process finished.")
    except FileNotFoundError:
        print("\n'final_output.csv' not found. Please run the full pipeline first to generate it.")