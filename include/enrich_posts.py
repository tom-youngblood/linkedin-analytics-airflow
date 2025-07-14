from utils import get_unenriched_posts_from_db, scrape_post_media_info, prepare_media_enrichment_data, finalize_enrichment_output, ingest_enriched_data_to_db
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
    # Get unenriched posts from database
    logger.info("Running get_unenriched_posts_from_db...")
    unenriched_posts_df = get_unenriched_posts_from_db()
    if unenriched_posts_df.empty:
        logger.info("No unenriched posts to process. Task completed successfully.")
        return
    logger.info("get_unenriched_posts_from_db run complete")

    # Scrape post media and additional metrics
    logger.info("Running prepare_media_enrichment_data...")
    media_enrichment_df = prepare_media_enrichment_data(unenriched_posts_df)
    if media_enrichment_df.empty:
        logger.info("No media enrichment data. Task completed successfully.")
        return
    logger.info("prepare_media_enrichment_data run complete")

    # Produce final output
    logger.info("Running finalize_enrichment_output...")
    final_enrichment_df = finalize_enrichment_output(unenriched_posts_df, media_enrichment_df)
    if final_enrichment_df.empty:
        logger.info("No final output to ingest. Task completed successfully.")
        return
    logger.info("finalize_enrichment_output run complete")

    # Ingest final output to PostgreSQL
    logger.info("Running ingest_enriched_data_to_db...")
    ingest_enriched_data_to_db(final_enrichment_df)
    logger.info("ingest_enriched_data_to_db complete")

if __name__ == "__main__":
    main()