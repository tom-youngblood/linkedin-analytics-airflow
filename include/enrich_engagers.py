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
        AND (engager_bucketed_position IS NULL OR engager_bucketed_position = '')
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
    Use OpenAI API to classify an engager's audience and position based on their company and title.
    
    Args:
        company (str): The company name
        title (str): The job title
        name (str, optional): The person's name
        headline (str, optional): The LinkedIn headline
        
    Returns:
        tuple: (audience, position) where audience is one of the three groups and position is the specific role
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
        Based on the following professional information, classify this person into one of these three audience categories and their specific position:

        {context}

        TARGET AUDIENCE CATEGORIES (hierarchically ordered by importance, WITHIN each group; groups themselves are not hierarchically ordered.):

        Group 1: Venture Capital Related
        - Partner at a VC firm
        - Principal at a VC firm  
        - Associate at a VC firm
        - Other at a VC firm

        Group 2: Venture Capital Backed Startup
        - CEO, Founder, or CoFounder at a Venture Capital Backed Startup
        - Chief, Executive, Director, or Manager Level position at a venture backed startup
        - Marketing Position at a venture backed startup
        - Revenue Position at a venture backed startup
        - Sales Position at a venture backed startup
        - Demand Gen Position at a venture backed startup
        - Other at a venture backed startup

        Group 3: Marketing Agency
        - CEO, Founder, or CoFounder at a Marketing Agency
        - Chief, Executive, Director, or Manager Level position at a marketing agency
        - Partnerships at a Marketing Agency
        - Other at a marketing agency

        INSTRUCTIONS:
        1. First determine which GROUP the person belongs to (Venture Capital Related, Venture Capital Backed Startup, or Marketing Agency)
        2. Then determine their specific POSITION within that group
        3. If they don't clearly fit any group, classify as "Other" for both audience and position

        Respond with ONLY a JSON object in this exact format:
        {{"audience": "GROUP_NAME", "position": "POSITION_NAME"}}

        Examples:
        - {{"audience": "Venture Capital Related", "position": "Partner"}}
        - {{"audience": "Marketing Agency", "position": "CEO, Founder, or CoFounder at a Marketing Agency"}}
        - {{"audience": "Venture Capital Backed Startup", "position": "Other"}}
        - {{"audience": "Other", "position": "Other"}}
        """

        # Make the API call
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert at classifying professionals into target audience categories for B2B marketing."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        # Extract and parse the response
        response_text = response.choices[0].message.content.strip()
        
        # Parse JSON response
        import json
        classification = json.loads(response_text)
        
        audience = classification.get("audience", "Other")
        position = classification.get("position", "Other")
        
        # Validate the response
        valid_audiences = ["Venture Capital Related", "Venture Capital Backed Startup", "Marketing Agency", "Other"]
        if audience not in valid_audiences:
            logger.warning(f"Invalid audience classification '{audience}' for {company}/{title}. Defaulting to 'Other'")
            audience = "Other"
            position = "Other"
        
        logger.info(f"Classified {company}/{title} as: {audience} - {position}")
        return audience, position
        
    except Exception as e:
        logger.error(f"Error classifying audience for {company}/{title}: {str(e)}")
        return "Other", "Other"

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
            audience, position = classify_audience_with_openai(company, title, name, headline)
            
            # Update the database
            cursor.execute(
                """
                UPDATE linkedin_engagers
                SET engager_audience = %s, engager_bucketed_position = %s
                WHERE id = %s
                """,
                (audience, position, engager_id)
            )
            
            update_count += 1
            logger.info(f"Updated engager ID {engager_id} with audience: {audience} - {position}")
            
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
                COUNT(CASE WHEN engager_audience IS NOT NULL AND engager_audience != '' THEN 1 END) as enriched_count
            FROM linkedin_engagers
        """)
        
        total_stats = cursor.fetchone()
        logger.info(f"Total engagers: {total_stats['total_engagers']}")
        logger.info(f"Enriched engagers: {total_stats['enriched_count']}")
        
        # Audience distribution
        cursor.execute("""
            SELECT 
                engager_audience,
                COUNT(*) as count
            FROM linkedin_engagers
            WHERE engager_audience IS NOT NULL AND engager_audience != ''
            GROUP BY engager_audience
            ORDER BY count DESC
        """)
        
        audience_results = cursor.fetchall()
        logger.info("Audience classification statistics:")
        for row in audience_results:
            logger.info(f"  {row['engager_audience']}: {row['count']} engagers")
        
        # Position distribution within each audience
        cursor.execute("""
            SELECT 
                engager_audience,
                engager_bucketed_position,
                COUNT(*) as count
            FROM linkedin_engagers
            WHERE engager_audience IS NOT NULL AND engager_audience != ''
            AND engager_bucketed_position IS NOT NULL AND engager_bucketed_position != ''
            GROUP BY engager_audience, engager_bucketed_position
            ORDER BY engager_audience, count DESC
        """)
        
        position_results = cursor.fetchall()
        logger.info("Position classification statistics:")
        for row in position_results:
            logger.info(f"  {row['engager_audience']} - {row['engager_bucketed_position']}: {row['count']} engagers")
        
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