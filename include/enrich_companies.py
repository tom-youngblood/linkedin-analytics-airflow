import logging
import pandas as pd
import psycopg2
from utils import scrape_company, get_db_connection, get_db_cursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/enrich_companies.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def ensure_columns_exist(cursor):
    # Add 'company' column if it doesn't exist
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='linkedin_engagers' AND column_name='company'
            ) THEN
                ALTER TABLE linkedin_engagers ADD COLUMN company TEXT;
            END IF;
        END$$;
    """)
    # Add 'title' column if it doesn't exist
    cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='linkedin_engagers' AND column_name='title'
            ) THEN
                ALTER TABLE linkedin_engagers ADD COLUMN title TEXT;
            END IF;
        END$$;
    """)

def ensure_companies_table_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_companies (
            id SERIAL PRIMARY KEY,
            company_name TEXT,
            company_url TEXT UNIQUE
        );
    """)

def migrate_companies_from_engagers(cursor, conn):
    # Find all company profiles in engagers
    cursor.execute("""
        SELECT DISTINCT name, linkedin_url FROM linkedin_engagers
        WHERE linkedin_url LIKE '%/company/%'
    """)
    companies = cursor.fetchall()
    logger.info(f"Found {len(companies)} company profiles to migrate from engagers.")
    inserted = 0
    for name, url in companies:
        # Insert into linkedin_companies table if not exists
        cursor.execute(
            """
            INSERT INTO linkedin_companies (company_name, company_url)
            VALUES (%s, %s)
            ON CONFLICT (company_url) DO NOTHING
            """,
            (name, url)
        )
        inserted += cursor.rowcount
    conn.commit()
    logger.info(f"Inserted {inserted} new companies into linkedin_companies table.")
    # Delete from engagers
    cursor.execute("""
        DELETE FROM linkedin_engagers WHERE linkedin_url LIKE '%/company/%'
    """)
    deleted = cursor.rowcount
    conn.commit()
    logger.info(f"Deleted {deleted} company profiles from linkedin_engagers.")

def get_unenriched_engagers(cursor):
    query = """
        SELECT id, linkedin_url FROM linkedin_engagers
        WHERE company IS NULL OR company = '' OR title IS NULL OR title = ''
        AND linkedin_url IS NOT NULL AND linkedin_url != ''
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=["id", "linkedin_url"])

def enrich_and_update_engagers(df, cursor, conn):
    for _, row in df.iterrows():
        linkedin_url = row["linkedin_url"]
        engager_id = row["id"]
        try:
            logger.info(f"Enriching {linkedin_url} (id={engager_id})...")
            result = scrape_company(linkedin_url)
            company = result.get("company")
            title = result.get("title")
            # Update the row in the database
            cursor.execute(
                """
                UPDATE linkedin_engagers
                SET company = %s, title = %s
                WHERE id = %s
                """,
                (company, title, engager_id)
            )
            conn.commit()
            logger.info(f"Updated id={engager_id} with company='{company}', title='{title}'")
        except Exception as e:
            logger.error(f"Failed to enrich {linkedin_url} (id={engager_id}): {str(e)}")

def main():
    logger.info("Starting company/title enrichment for linkedin_engagers and company migration...")
    conn = get_db_connection()
    cursor = get_db_cursor(conn)
    try:
        ensure_columns_exist(cursor)
        ensure_companies_table_exists(cursor)
        conn.commit()
        migrate_companies_from_engagers(cursor, conn)
        unenriched_df = get_unenriched_engagers(cursor)
        logger.info(f"Found {len(unenriched_df)} engagers to enrich.")
        if not unenriched_df.empty:
            enrich_and_update_engagers(unenriched_df, cursor, conn)
        else:
            logger.info("No engagers need enrichment.")
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
