from utils import scrape_posts_by_profile, scrape_post_media_info, prepare_media_enrichment_data, finalize_enrichment_output, ingest_enriched_data_to_db
import logging

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

def main():
    # Scrape posts by profile
    logger.info("Running scrape_posts_by_profile...")
    scrape_posts_by_profile_df = scrape_posts_by_profile()
    if scrape_posts_by_profile_df.empty:
        logger.info("No posts to process. Exiting pipeline.")
        exit(0)
    logger.info("scrape_posts_by_profile run complete")

    # Scrape post media and additional metrics
    logger.info("Running prepare_media_enrichment_data...")
    prepare_media_enrichment_data_df = prepare_media_enrichment_data(scrape_posts_by_profile_df)
    if prepare_media_enrichment_data_df.empty:
        logger.info("No media enrichment data. Exiting pipeline.")
        exit(0)
    logger.info("prepare_media_enrichment_data run complete")

    # Produce final output
    logger.info("Running finalize_enrichment_output_df...")
    finalize_enrichment_output_df = finalize_enrichment_output(scrape_posts_by_profile_df, prepare_media_enrichment_data_df)
    if finalize_enrichment_output_df.empty:
        logger.info("No final output to ingest. Exiting pipeline.")
        exit(0)
    logger.info("finalize_enrichment_output_df run complete")

    # Ingest final output to PostgreSQL
    logger.info("Running ingest_enriched_data_to_db...")
    ingest_enriched_data_to_db(finalize_enrichment_output_df)
    logger.info("ingest_enriched_data_to_db complete")

if __name__=="__main__":
    main()