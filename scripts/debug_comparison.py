import pandas as pd
import logging
import os
from dotenv import load_dotenv
import utils
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/debug_comparison.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_database_data():
    """
    Load all posts from the PostgreSQL database.
    
    Returns:
        pandas.DataFrame: DataFrame containing all posts from the database
    """
    logger.info("Loading data from PostgreSQL database...")
    try:
        conn = utils.get_db_connection()
        
        # Query to get all posts with their enrichment status
        query = """
        SELECT 
            id,
            post_url,
            post_name,
            text,
            post_type,
            comments,
            reposts,
            reshared_post_url,
            reshared_post_total_reactions,
            media_type,
            media_url,
            article_url,
            article_title,
            duration,
            mime_type,
            thumbnail,
            video_url,
            image_url,
            enriched,
            enriched_time,
            last_scraped_at,
            scrape_count,
            total_reactions
        FROM linkedin_posts
        ORDER BY id
        """
        
        db_df = pd.read_sql_query(query, conn)
        conn.close()
        
        logger.info(f"Loaded {len(db_df)} posts from database")
        logger.info(f"Database columns: {list(db_df.columns)}")
        
        return db_df
        
    except Exception as e:
        logger.error(f"Error loading database data: {str(e)}")
        raise

def load_csv_data():
    """
    Load data from the final_output.csv file.
    
    Returns:
        pandas.DataFrame: DataFrame containing data from the CSV file
    """
    logger.info("Loading data from final_output.csv...")
    try:
        csv_path = "final_output.csv"
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return pd.DataFrame()
        
        csv_df = pd.read_csv(csv_path)
        
        logger.info(f"Loaded {len(csv_df)} rows from CSV")
        logger.info(f"CSV columns: {list(csv_df.columns)}")
        
        return csv_df
        
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        raise

def clean_urls_for_comparison(df, url_column):
    """
    Clean URLs by removing query parameters for comparison.
    
    Args:
        df (pandas.DataFrame): DataFrame containing URLs
        url_column (str): Name of the column containing URLs
    
    Returns:
        pandas.DataFrame: DataFrame with cleaned URLs
    """
    df_clean = df.copy()
    
    def clean_url(url):
        if pd.isna(url) or not isinstance(url, str):
            return url
        # Remove everything after '?' (query parameters)
        return url.split('?')[0]
    
    df_clean[f'{url_column}_clean'] = df_clean[url_column].apply(clean_url)
    return df_clean

def compare_data(db_df, csv_df):
    """
    Compare database and CSV data to identify discrepancies.
    
    Args:
        db_df (pandas.DataFrame): Database data
        csv_df (pandas.DataFrame): CSV data
    """
    logger.info("Starting data comparison...")
    
    # Clean URLs for comparison
    db_df_clean = clean_urls_for_comparison(db_df, 'post_url')
    csv_df_clean = clean_urls_for_comparison(csv_df, 'url')
    
    # Create sets of URLs for comparison
    db_urls = set(db_df_clean['post_url_clean'].dropna())
    csv_urls = set(csv_df_clean['url_clean'].dropna())
    
    logger.info(f"Database has {len(db_urls)} unique URLs")
    logger.info(f"CSV has {len(csv_urls)} unique URLs")
    
    # Find URLs in CSV but not in database
    csv_only = csv_urls - db_urls
    logger.info(f"URLs in CSV but not in database: {len(csv_only)}")
    if csv_only:
        logger.info(f"Sample CSV-only URLs: {list(csv_only)[:5]}")
    
    # Find URLs in database but not in CSV
    db_only = db_urls - csv_urls
    logger.info(f"URLs in database but not in CSV: {len(db_only)}")
    if db_only:
        logger.info(f"Sample DB-only URLs: {list(db_only)[:5]}")
    
    # Find common URLs
    common_urls = db_urls & csv_urls
    logger.info(f"Common URLs: {len(common_urls)}")
    
    # For common URLs, compare enrichment status
    if common_urls:
        logger.info("Analyzing enrichment status for common URLs...")
        
        # Get database data for common URLs
        db_common = db_df_clean[db_df_clean['post_url_clean'].isin(common_urls)]
        csv_common = csv_df_clean[csv_df_clean['url_clean'].isin(common_urls)]
        
        # Check enrichment status
        enriched_in_db = db_common[db_common['enriched'] == True]
        logger.info(f"Posts enriched in database: {len(enriched_in_db)}")
        
        # Check for posts that should be enriched but aren't
        should_be_enriched = db_common[db_common['enriched'] == False]
        logger.info(f"Posts not enriched in database: {len(should_be_enriched)}")
        
        if len(should_be_enriched) > 0:
            logger.info("Sample posts that should be enriched:")
            for _, row in should_be_enriched.head().iterrows():
                logger.info(f"  - {row['post_url']} (ID: {row['id']})")
    
    return {
        'csv_only': csv_only,
        'db_only': db_only,
        'common_urls': common_urls,
        'enriched_count': len(enriched_in_db) if 'enriched_in_db' in locals() else 0,
        'unenriched_count': len(should_be_enriched) if 'should_be_enriched' in locals() else 0
    }

def detailed_url_analysis(db_df, csv_df):
    """
    Perform detailed analysis of URL matching issues.
    
    Args:
        db_df (pandas.DataFrame): Database data
        csv_df (pandas.DataFrame): CSV data
    """
    logger.info("Performing detailed URL analysis...")
    
    # Sample some URLs from both sources for detailed comparison
    db_sample = db_df['post_url'].dropna().head(10).tolist()
    csv_sample = csv_df['url'].dropna().head(10).tolist()
    
    logger.info("Sample database URLs:")
    for url in db_sample:
        logger.info(f"  DB: {url}")
    
    logger.info("Sample CSV URLs:")
    for url in csv_sample:
        logger.info(f"  CSV: {url}")
    
    # Check for exact matches
    exact_matches = set(db_sample) & set(csv_sample)
    logger.info(f"Exact URL matches in sample: {len(exact_matches)}")
    
    # Check for cleaned matches
    db_clean_sample = [url.split('?')[0] for url in db_sample if url]
    csv_clean_sample = [url.split('?')[0] for url in csv_sample if url]
    cleaned_matches = set(db_clean_sample) & set(csv_clean_sample)
    logger.info(f"Cleaned URL matches in sample: {len(cleaned_matches)}")

def analyze_enrichment_columns(db_df, csv_df):
    """
    Analyze the enrichment columns to see what data is available.
    
    Args:
        db_df (pandas.DataFrame): Database data
        csv_df (pandas.DataFrame): CSV data
    """
    logger.info("Analyzing enrichment columns...")
    
    # Check CSV enrichment columns
    enrichment_columns = ['text', 'post_type', 'comments', 'reposts', 'media_type', 'media_url']
    
    logger.info("CSV enrichment data summary:")
    for col in enrichment_columns:
        if col in csv_df.columns:
            non_null_count = csv_df[col].notna().sum()
            total_count = len(csv_df)
            logger.info(f"  {col}: {non_null_count}/{total_count} non-null values")
        else:
            logger.warning(f"  {col}: Column not found in CSV")
    
    # Check database enrichment columns
    logger.info("Database enrichment data summary:")
    for col in enrichment_columns:
        if col in db_df.columns:
            non_null_count = db_df[col].notna().sum()
            total_count = len(db_df)
            logger.info(f"  {col}: {non_null_count}/{total_count} non-null values")
        else:
            logger.warning(f"  {col}: Column not found in database")

def main():
    """
    Main function to run the debugging comparison.
    """
    logger.info("Starting debugging comparison...")
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Load data from both sources
        db_df = load_database_data()
        csv_df = load_csv_data()
        
        if db_df.empty:
            logger.error("No data loaded from database")
            return
        
        if csv_df.empty:
            logger.error("No data loaded from CSV")
            return
        
        # Perform comparisons
        comparison_results = compare_data(db_df, csv_df)
        
        # Detailed analysis
        detailed_url_analysis(db_df, csv_df)
        analyze_enrichment_columns(db_df, csv_df)
        
        # Summary
        logger.info("=== COMPARISON SUMMARY ===")
        logger.info(f"Database posts: {len(db_df)}")
        logger.info(f"CSV posts: {len(csv_df)}")
        logger.info(f"Common URLs: {len(comparison_results['common_urls'])}")
        logger.info(f"CSV-only URLs: {len(comparison_results['csv_only'])}")
        logger.info(f"Database-only URLs: {len(comparison_results['db_only'])}")
        logger.info(f"Enriched in database: {comparison_results['enriched_count']}")
        logger.info(f"Not enriched in database: {comparison_results['unenriched_count']}")
        
        # Recommendations
        logger.info("=== RECOMMENDATIONS ===")
        if len(comparison_results['csv_only']) > 0:
            logger.info("1. Some URLs in CSV are not in database - check URL cleaning logic")
        if len(comparison_results['db_only']) > 0:
            logger.info("2. Some URLs in database are not in CSV - check scraping logic")
        if comparison_results['unenriched_count'] > 0:
            logger.info("3. Some posts are not enriched - check ingestion logic")
        
    except Exception as e:
        logger.error(f"Error during debugging: {str(e)}")
        raise

if __name__ == "__main__":
    main() 