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
from enrich_engagers import ensure_required_columns_exist

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

        ensure_required_columns_exist(cursor)
        
        query = """
            SELECT id, linkedin_url, name, headline, company, title
            FROM linkedin_engagers
            WHERE company IS NOT NULL AND company != ''
            AND title IS NOT NULL AND title != ''
            AND linkedin_url IS NOT NULL AND linkedin_url != ''
            AND (engager_audience IS NULL OR engager_audience = '')
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
            
            # Classify the audience and position
            audience, position = classify_audience_with_openai(company, title, name, headline)
            
            # Store result
            result = {
                "id": engager_id,
                "company": company,
                "title": title,
                "name": name,
                "headline": headline,
                "classified_audience": audience,
                "classified_position": position
            }
            results.append(result)
            
            logger.info(f"ID {engager_id}: {company} - {title} → {audience} - {position}")
        
        # Print results in a format that can be easily shared
        print("\n" + "="*100)
        print("OPENAI-BASED CLASSIFICATION RESULTS")
        print("="*100)
        print("Please review these classifications and provide feedback on accuracy.")
        print("Format: ID | Company | Title | Name | Headline | Audience | Position")
        print("-"*100)
        
        for result in results:
            print(f"{result['id']} | {result['company']} | {result['title']} | {result['name']} | {result['headline']} | {result['classified_audience']} | {result['classified_position']}")
        
        print("-"*100)
        print("AUDIENCE DISTRIBUTION:")
        
        # Count audience classifications
        audience_counts = {}
        for result in results:
            audience = result['classified_audience']
            audience_counts[audience] = audience_counts.get(audience, 0) + 1
        
        for audience, count in sorted(audience_counts.items()):
            percentage = (count / len(results)) * 100
            print(f"  {audience}: {count} ({percentage:.1f}%)")
        
        print("\nPOSITION DISTRIBUTION:")
        
        # Count position classifications
        position_counts = {}
        for result in results:
            position = result['classified_position']
            position_counts[position] = position_counts.get(position, 0) + 1
        
        for position, count in sorted(position_counts.items()):
            percentage = (count / len(results)) * 100
            print(f"  {position}: {count} ({percentage:.1f}%)")
        
        print("\nDETAILED BREAKDOWN BY AUDIENCE:")
        
        # Detailed breakdown by audience
        audience_position_counts = {}
        for result in results:
            audience = result['classified_audience']
            position = result['classified_position']
            if audience not in audience_position_counts:
                audience_position_counts[audience] = {}
            audience_position_counts[audience][position] = audience_position_counts[audience].get(position, 0) + 1
        
        for audience in sorted(audience_position_counts.keys()):
            print(f"\n  {audience}:")
            for position, count in sorted(audience_position_counts[audience].items(), key=lambda x: x[1], reverse=True):
                print(f"    {position}: {count}")
        
        print("\n" + "="*100)
        print("INSTRUCTIONS FOR ACCURACY FEEDBACK:")
        print("="*100)
        print("1. Review each classification above")
        print("2. Note any incorrect classifications")
        print("3. Provide feedback in this format:")
        print("   ID: [correct_audience] - [correct_position] - [reason if needed]")
        print("   Example: 123: Venture Capital Related - Partner - This is correct")
        print("   Example: 456: Venture Capital Backed Startup - CEO - Should be Marketing Agency - CEO")
        print("4. Send the feedback so I can calculate accuracy metrics")
        print("="*100)
        
        # Calculate estimated cost
        estimated_cost = len(results) * 0.00015  # Approximate cost per classification
        print(f"\nEstimated OpenAI API cost for {len(results)} classifications: ${estimated_cost:.4f}")
        
        logger.info(f"Classification test completed for {len(results)} engagers")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_openai_classification() 