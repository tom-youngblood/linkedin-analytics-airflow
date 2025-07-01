"""
## LinkedIn Lead Generation Pipeline DAG

This DAG orchestrates the LinkedIn lead generation pipeline with four main tasks:
1. Google Sheets Sync: Pulls LinkedIn post data from Google Sheets into PostgreSQL
2. Post Enrichment: Enriches posts with media details and additional metrics
3. LinkedIn Scraping: Scrapes engagement data from LinkedIn posts using Apify
4. HubSpot Sync: Pushes new leads from PostgreSQL to HubSpot

The pipeline follows a sequential workflow where each task depends on the previous one.
All database operations and state management are handled within the individual scripts.

Schedule: Daily at 9 AM with random delay (runs between 9:00-10:00 AM)

For more information about the pipeline architecture, see the project README.
"""

import sys
import os
import random
import time
from datetime import timedelta
from airflow.decorators import dag, task
from pendulum import datetime
import logging

# Add the include directory to the Python path so we can import our scripts
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'include'))

# Import our pipeline scripts
import gs_sql
import enrich_posts
import scrape
import sql_hs

logger = logging.getLogger(__name__)

@dag(
    start_date=datetime(2025, 6, 26),
    # schedule="0 16 * * *",  # Removed schedule to disable automatic runs
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Coefficient Labs",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2)
    },
    tags=["linkedin", "lead-generation", "pipeline"],
)
def linkedin_lead_pipeline():
    """
    Main LinkedIn Lead Generation Pipeline DAG.
    
    This DAG orchestrates the complete workflow from Google Sheets to HubSpot:
    1. Sync LinkedIn post data from Google Sheets to PostgreSQL
    2. Enrich posts with media details and metrics
    3. Scrape engagement data from LinkedIn posts
    4. Push new leads to HubSpot
    
    Schedule: Runs daily at 9 AM with random delay (9:00-10:00 AM)
    """
    
    @task(
        task_id="random_delay"
    )
    def add_random_delay(**context):
        """
        Task to add a random delay between 0-60 minutes.
        This ensures the pipeline runs at a random time between 9:00-10:00 AM.
        """
        # Generate random delay between 0 and 60 minutes (0-3600 seconds)
        delay_seconds = random.randint(0, 3600)
        delay_minutes = delay_seconds / 60
        
        logger.info(f"Adding random delay of {delay_minutes:.1f} minutes ({delay_seconds} seconds)")
        
        # Sleep for the random delay
        time.sleep(delay_seconds)
        
        logger.info(f"Random delay completed. Pipeline starting now.")
        return {"delay_seconds": delay_seconds, "delay_minutes": delay_minutes}
    
    @task(
        task_id="google_sheets_sync"
    )
    def sync_google_sheets(**context):
        """
        Task to sync LinkedIn post data from Google Sheets into PostgreSQL.
        
        This task:
        - Connects to Google Sheets using service account credentials
        - Retrieves LinkedIn post URLs and names from the 'LI Links' worksheet
        - Creates/updates database tables if they don't exist
        - Ingests new posts and updates existing ones
        - Handles deduplication and logging
        """
        try:
            logger.info("Starting Google Sheets sync task...")
            gs_sql.main()
            logger.info("Google Sheets sync completed successfully")
            return {"status": "success", "message": "Google Sheets sync completed"}
        except Exception as e:
            logger.error(f"Google Sheets sync failed: {str(e)}")
            raise

    @task(
        task_id="enrich_posts"
    )
    def enrich_posts_data(**context):
        """
        Task to enrich LinkedIn posts with media details and additional metrics.
        
        This task:
        - Scrapes posts by profile to get basic post information
        - Prepares media enrichment data (video duration, thumbnails, etc.)
        - Finalizes enrichment output with all metrics
        - Ingests enriched data to PostgreSQL database
        """
        try:
            logger.info("Starting post enrichment task...")
            enrich_posts.main()
            logger.info("Post enrichment completed successfully")
            return {"status": "success", "message": "Post enrichment completed"}
        except Exception as e:
            logger.error(f"Post enrichment failed: {str(e)}")
            raise

    @task(
        task_id="scrape_linkedin"
    )
    def scrape_linkedin_posts(**context):
        """
        Task to scrape LinkedIn post engagement data.
        
        This task:
        - Queries database for posts that need scraping (based on scrape count and cooldown)
        - Scrapes engagement data using Apify API
        - Updates the scrapes table with results
        - Handles rate limiting with random delays between scrapes
        """
        try:
            logger.info("Starting LinkedIn scraping task...")
            scrape.main()
            logger.info("LinkedIn scraping completed successfully")
            return {"status": "success", "message": "LinkedIn scraping completed"}
        except Exception as e:
            logger.error(f"LinkedIn scraping failed: {str(e)}")
            raise

    @task(
        task_id="sync_hubspot"
    )
    def sync_hubspot_leads(**context):
        """
        Task to sync new leads from PostgreSQL to HubSpot.
        
        This task:
        - Queries local database for engagers not yet in HubSpot
        - Fetches existing contacts from HubSpot Organic Social list
        - Deduplicates leads to avoid duplicates
        - Pushes new contacts to HubSpot with proper property mapping
        """
        try:
            logger.info("Starting HubSpot sync task...")
            sql_hs.main()
            logger.info("HubSpot sync completed successfully")
            return {"status": "success", "message": "HubSpot sync completed"}
        except Exception as e:
            logger.error(f"HubSpot sync failed: {str(e)}")
            raise

    # Define the task dependencies - sequential execution with random delay
    # delay_result = add_random_delay()  # Commented out for testing
    sheets_result = sync_google_sheets()
    enrichment_result = enrich_posts_data()
    scraping_result = scrape_linkedin_posts()
    hubspot_result = sync_hubspot_leads()
    
    # Set up the dependency chain with random delay at the start
    # delay_result >> sheets_result >> scraping_result >> enrichment_result >> hubspot_result  # Commented out for testing
    sheets_result >> scraping_result >> enrichment_result >> hubspot_result


# Instantiate the DAG
linkedin_lead_pipeline()
