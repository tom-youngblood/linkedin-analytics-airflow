"""
## LinkedIn Lead Generation Pipeline DAG

This DAG orchestrates the LinkedIn lead generation pipeline with a new, streamlined workflow.
HubSpot is now the single source of truth for contact and company data.

The pipeline has four main tasks:
1. Database Schema Check: Ensures the RDS schema (for post and scrape data) is correct.
2. LinkedIn Scraping: Scrapes engagement data from LinkedIn posts into RDS.
3. Post Enrichment: Enriches post data (media details, etc.) within RDS.
4. HubSpot Sync & Enrich: Creates new contacts in HubSpot and enriches them directly with company, title, and audience data.

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
import setup_database
import scrape
import enrich_posts
import sync_sql_to_hubspot
import enrich_hubspot_contacts

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
    
    This DAG orchestrates the complete workflow from database setup to HubSpot:
    1. Ensure database schema is up-to-date
    2. Scrape engagement data from LinkedIn posts using Apify
    3. Enrich posts with media details and metrics
    4. Sync and enrich contacts in HubSpot
    
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
        delay_seconds = random.randint(0, 3600)
        delay_minutes = delay_seconds / 60
        logger.info(f"Adding random delay of {delay_minutes:.1f} minutes ({delay_seconds} seconds)")
        time.sleep(delay_seconds)
        logger.info("Random delay completed. Pipeline starting now.")
        return {"delay_seconds": delay_seconds, "delay_minutes": delay_minutes}
    
    @task(
        task_id="ensure_database_schema"
    )
    def ensure_database_schema(**context):
        """
        Task to ensure the database schema is correctly set up.
        This task runs an idempotent script that prepares the RDS database.
        """
        try:
            logger.info("Starting database schema setup task...")
            setup_database.main()
            logger.info("Database schema setup completed successfully.")
            return {"status": "success", "message": "Database schema is up to date."}
        except Exception as e:
            logger.error(f"Database schema setup failed: {str(e)}")
            raise

    @task(
        task_id="scrape_linkedin"
    )
    def scrape_linkedin_posts(**context):
        """
        Task to scrape LinkedIn post engagement data into RDS.
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
        task_id="enrich_posts"
    )
    def enrich_posts_data(**context):
        """
        Task to enrich LinkedIn posts in RDS with media details and other metrics.
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
        task_id="sync_sql_to_hubspot"
    )
    def sync_sql_to_hubspot_task(**context):
        """
        Task to sync new leads to HubSpot (creates new contacts in HubSpot).
        """
        try:
            logger.info("Starting HubSpot sync task...")
            sync_sql_to_hubspot.main()
            logger.info("HubSpot sync completed successfully")
            return {"status": "success", "message": "HubSpot sync completed"}
        except Exception as e:
            logger.error(f"HubSpot sync failed: {str(e)}")
            raise

    @task(
        task_id="enrich_hubspot_contacts"
    )
    def enrich_hubspot_contacts_task(**context):
        """
        Task to enrich HubSpot contacts with company, title, and audience data.
        """
        try:
            logger.info("Starting HubSpot contact enrichment task...")
            enrich_hubspot_contacts.main()
            logger.info("HubSpot contact enrichment completed successfully")
            return {"status": "success", "message": "HubSpot contact enrichment completed"}
        except Exception as e:
            logger.error(f"HubSpot contact enrichment failed: {str(e)}")
            raise

    # Define the task dependencies for the new streamlined pipeline
    schema_result = ensure_database_schema()
    scraping_result = scrape_linkedin_posts()
    posts_result = enrich_posts_data()
    sync_result = sync_sql_to_hubspot_task()
    enrich_result = enrich_hubspot_contacts_task()
    
    # Set up the dependency chain
    schema_result >> scraping_result >> posts_result >> sync_result >> enrich_result

# Instantiate the DAG
linkedin_lead_pipeline()