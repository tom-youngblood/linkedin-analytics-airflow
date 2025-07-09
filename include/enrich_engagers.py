import logging
import pandas as pd
import openai
from openai import OpenAI
from utils import get_db_connection, get_db_cursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/enrich_engagers.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY not found in environment variables")
    raise ValueError("OPENAI_API_KEY is required")

# Configure OpenAI client
client = openai.OpenAI(api_key=OPENAI_API_KEY)
logger.info("OpenAI client configured successfully")

def ensure_engager_audience_column_exists(cursor):
    """
    Ensure the engager_audience column exists in the linkedin_engagers table.
    """
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='linkedin_engagers' AND column_name='engager_audience'
            ) THEN
                ALTER TABLE linkedin_engagers ADD COLUMN engager_audience TEXT;
            END IF;
        END$$;
    """)
    logger.info("Ensured engager_audience column exists in linkedin_engagers table")

def get_unenriched_engagers(cursor):
    """
    Fetch engagers that don't have engager_audience populated but have company and title data.
    
    Returns:
        pandas.DataFrame: DataFrame with columns [id, linkedin_url, name, headline, company, title]
    """
    query = """
        SELECT id, linkedin_url, name, headline, company, title
        FROM linkedin_engagers
        WHERE (engager_audience IS NULL OR engager_audience = '')
        AND company IS NOT NULL AND company != ''
        AND title IS NOT NULL AND title != ''
        AND linkedin_url IS NOT NULL AND linkedin_url != ''
        LIMIT 100  -- Process in batches to avoid rate limits
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["id", "linkedin_url", "name", "headline", "company", "title"])
    logger.info(f"Found {len(df)} engagers to enrich with audience classification")
    return df

def classify_audience_with_openai(company, title, name=None, headline=None):
    """
    Use OpenAI API to classify an engager's audience based on their company and title.
    
    Args:
        company (str): The company name
        title (str): The job title
        name (str, optional): The person's name
        headline (str, optional): The LinkedIn headline
        
    Returns:
        str: One of the three audience types or "Other"
    """
    try:
        
        # Prepare the context for classification
        context = f"Company: {company}\nTitle: {title}"
        if name:
            context += f"\nName: {name}"
        if headline:
            context += f"\nHeadline: {headline}"
        
        # Define the classification prompt
        prompt = f"""
        Based on the following professional information, classify this person into one of these three audience categories:

        {context}

        Audience Categories:
        1. "VC" - Venture Capitalists, investors, or people working at venture capital firms
        2. "Agency Owners" - People who own or run marketing agencies, consulting firms, or service businesses
        3. "VC-backed Startup Founder / Marketing Exec" - Founders of venture-backed startups or marketing executives at such companies

        If the person doesn't clearly fit into any of these three categories, classify them as "Other".

        Respond with ONLY the category name (VC, Agency Owners, VC-backed Startup Founder / Marketing Exec, or Other).
        """

        # Make the API call using the modern syntax
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Extract and clean the response
        audience = response.choices[0].message.content.strip()
        
        # Validate the response
        valid_audiences = ["VC", "Agency Owners", "VC-backed Startup Founder / Marketing Exec", "Other"]
        if audience not in valid_audiences:
            logger.warning(f"Invalid audience classification '{audience}' for {company}/{title}. Defaulting to 'Other'")
            audience = "Other"
        
        logger.info(f"Classified {company}/{title} as: {audience}")
        return audience
        
    except Exception as e:
        logger.error(f"Error classifying audience for {company}/{title}: {str(e)}")
        return "Other"

def enrich_and_update_engagers(df, cursor, conn):
    """
    Enrich engagers with audience classification and update the database.
    
    Args:
        df (pandas.DataFrame): DataFrame of engagers to enrich
        cursor: Database cursor
        conn: Database connection
    """
    update_count = 0
    error_count = 0
    
    for _, row in df.iterrows():
        try:
            engager_id = row["id"]
            company = row["company"]
            title = row["title"]
            name = row.get("name")
            headline = row.get("headline")
            
            logger.info(f"Processing engager ID {engager_id}: {company} - {title}")
            
            # Classify the audience
            audience = classify_audience_with_openai(company, title, name, headline)
            
            # Update the database
            cursor.execute(
                """
                UPDATE linkedin_engagers
                SET engager_audience = %s
                WHERE id = %s
                """,
                (audience, engager_id)
            )
            
            update_count += 1
            logger.info(f"Updated engager ID {engager_id} with audience: {audience}")
            
            # Commit every 10 updates to avoid long transactions
            if update_count % 10 == 0:
                conn.commit()
                logger.info(f"Committed {update_count} updates so far")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Failed to enrich engager ID {row.get('id', 'unknown')}: {str(e)}")
            continue
    
    # Final commit
    conn.commit()
    logger.info(f"Enrichment complete. Successfully updated: {update_count}, Errors: {error_count}")

def main():
    """
    Main function to enrich LinkedIn engagers with audience classification using OpenAI.
    """
    logger.info("Starting engager audience enrichment process (OpenAI)...")
    
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        
        # Ensure the engager_audience column exists
        ensure_engager_audience_column_exists(cursor)
        conn.commit()
        
        # Get engagers that need enrichment
        unenriched_df = get_unenriched_engagers(cursor)
        
        if unenriched_df.empty:
            logger.info("No engagers found that need audience enrichment.")
            return
        
        # Enrich and update engagers
        enrich_and_update_engagers(unenriched_df, cursor, conn)
        
        # Log final statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_engagers,
                COUNT(CASE WHEN engager_audience IS NOT NULL AND engager_audience != '' THEN 1 END) as enriched_count,
                engager_audience,
                COUNT(*) as count
            FROM linkedin_engagers
            WHERE engager_audience IS NOT NULL AND engager_audience != ''
            GROUP BY engager_audience
            ORDER BY count DESC
        """)
        
        results = cursor.fetchall()
        logger.info("Audience classification statistics:")
        for row in results:
            logger.info(f"  {row['engager_audience']}: {row['count']} engagers")
        
    except Exception as e:
        logger.error(f"Error in main enrichment process: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main() 