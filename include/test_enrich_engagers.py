#!/usr/bin/env python3
"""
Test script for OpenAI-based engager audience classification using real database data.
This script tests the classification on the first 100 engagers from the database without making any database changes.
"""

import sys
import logging
import pandas as pd
import os
from dotenv import load_dotenv

# Add the include directory to the path
sys.path.append('include')

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY not found in environment variables")
    raise ValueError("OPENAI_API_KEY is required")

def get_real_engagers_from_db():
    """
    Fetch the first 100 engagers from the database that have company and title data.
    
    Returns:
        pandas.DataFrame: DataFrame with engager data
    """
    try:
        from utils import get_db_connection, get_db_cursor
        
        conn = get_db_connection()
        cursor = get_db_cursor(conn)
        
        query = """
            SELECT id, linkedin_url, name, headline, company, title
            FROM linkedin_engagers
            WHERE company IS NOT NULL AND company != ''
            AND title IS NOT NULL AND title != ''
            AND linkedin_url IS NOT NULL AND linkedin_url != ''
            ORDER BY id
            LIMIT 100
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=["id", "linkedin_url", "name", "headline", "company", "title"])
        
        cursor.close()
        conn.close()
        
        logger.info(f"Retrieved {len(df)} engagers from database")
        return df
        
    except Exception as e:
        logger.error(f"Failed to fetch engagers from database: {str(e)}")
        return pd.DataFrame()

def test_openai_classification():
    """
    Test the OpenAI-based audience classification function with real database data.
    """
    try:
        from enrich_engagers import classify_audience_with_openai
        
        # Get real engagers from database
        engagers_df = get_real_engagers_from_db()
        
        if engagers_df.empty:
            logger.error("No engagers found in database or database connection failed")
            return
        
        logger.info("Testing OpenAI-based audience classification with real database data...")
        
        # Store results for analysis
        results = []
        
        for index, row in engagers_df.iterrows():
            engager_id = row["id"]
            company = row["company"]
            title = row["title"]
            name = row.get("name", "")
            headline = row.get("headline", "")
            
            # Classify the audience
            audience = classify_audience_with_openai(company, title, name, headline)
            
            # Store result
            result = {
                "id": engager_id,
                "company": company,
                "title": title,
                "name": name,
                "headline": headline,
                "classified_audience": audience
            }
            results.append(result)
            
            logger.info(f"ID {engager_id}: {company} - {title} → {audience}")
        
        # Print results in a format that can be easily shared
        print("\n" + "="*80)
        print("OPENAI-BASED CLASSIFICATION RESULTS")
        print("="*80)
        print("Please review these classifications and provide feedback on accuracy.")
        print("Format: ID | Company | Title | Name | Headline | Classified Audience")
        print("-"*80)
        
        for result in results:
            print(f"{result['id']} | {result['company']} | {result['title']} | {result['name']} | {result['headline']} | {result['classified_audience']}")
        
        print("-"*80)
        print("AUDIENCE DISTRIBUTION:")
        
        # Count classifications
        audience_counts = {}
        for result in results:
            audience = result['classified_audience']
            audience_counts[audience] = audience_counts.get(audience, 0) + 1
        
        for audience, count in sorted(audience_counts.items()):
            percentage = (count / len(results)) * 100
            print(f"  {audience}: {count} ({percentage:.1f}%)")
        
        print("\n" + "="*80)
        print("INSTRUCTIONS FOR ACCURACY FEEDBACK:")
        print("="*80)
        print("1. Review each classification above")
        print("2. Note any incorrect classifications")
        print("3. Provide feedback in this format:")
        print("   ID: [correct_audience] - [reason if needed]")
        print("   Example: 123: VC - This is actually a startup founder")
        print("4. Send the feedback so I can calculate accuracy metrics")
        print("="*80)
        
        # Calculate estimated cost
        estimated_cost = len(results) * 0.00015  # Approximate cost per classification
        print(f"\nEstimated OpenAI API cost for {len(results)} classifications: ${estimated_cost:.4f}")
        
        logger.info(f"Classification test completed for {len(results)} engagers")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_openai_classification() 