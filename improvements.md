# Playbook: Refactoring the Pipeline for Production Readiness

## 1. Overview

This document is a step-by-step guide for refactoring the Python scripts in the `include/` directory. The goal is to evolve them from functional scripts into robust, testable, and production-hardened components of a modern data pipeline.

The core principles we will follow are:
1.  **Test-Driven Refactoring**: We will write tests *before* changing code to create a safety net.
2.  **Separation of Concerns**: We will isolate business logic from database operations.
3.  **Atomicity**: We will ensure that database updates are "all or nothing" to prevent inconsistent data states.
4.  **Idempotency**: We will design scripts so they can be run multiple times without causing errors or duplicate data.

We will use `include/enrich_companies.py` as our working example. This pattern can then be applied to `enrich_posts.py`, `gs_sql.py`, and other scripts.

---

## 2. The Refactoring Workflow

To refactor critical code safely, we will follow a precise, three-step process:

*   **Step 0: Create a Safety Net (Write a Test)**: Before touching the existing logic, we create an automated test that verifies its current behavior. This test will fail at first, but once it passes, it acts as a guarantee that our future refactoring doesn't break anything.
*   **Step 1: Refactor the Code**: With the safety net in place, we can confidently restructure the code to improve its design, making it more modular, efficient, and atomic.
*   **Step 2: Verify and Deploy**: We run our test against the new code to prove it works as expected. Once it passes, we can merge and deploy the changes.

---

## 3. Step-by-Step Guide: Refactoring `enrich_companies.py`

### Prerequisites: Install Testing Tools

First, add the necessary testing libraries to your `requirements.txt` and reinstall your dependencies.

```text
# Add these lines to requirements.txt
pytest
pytest-mock
```

Then, if you are inside the Astro development shell, run `pip install -r requirements.txt`. If not, restarting the environment with `astro dev start` will handle it.

### Step 0: Create the Safety Net Test

We will create a test that validates the core logic of the script without touching the live database or making real API calls.

1.  **Create the test file**:
    In the `tests/` directory, mirror the `include/` structure. Create a new file: `tests/include/test_enrich_companies.py`.

2.  **Write the test code**:
    Add the following code to the new test file. This test defines the desired behavior of our script.

    ```python
    # tests/include/test_enrich_companies.py
    import pytest
    import pandas as pd
    from unittest.mock import MagicMock

    # Import the functions we want to test from the script
    # Note: We will need to refactor the script to make these functions importable
    from include.enrich_companies import enrich_and_update_engagers

    @pytest.fixture
    def unenriched_df():
        """Provides a sample DataFrame of unenriched engagers."""
        data = {
            "linkedin_url": [
                "https://linkedin.com/in/person1",
                "https://linkedin.com/in/person2",
            ]
        }
        return pd.DataFrame(data)

    def test_enrich_and_update_engagers(unenriched_df, mocker):
        """
        Tests the end-to-end enrichment and database update logic.
        - Mocks the scraper to return fake data.
        - Mocks the database to verify the final UPDATE statement.
        """
        # 1. Mock the external dependencies
        mock_scrape_company = mocker.patch(
            "include.enrich_companies.scrape_company",
            side_effect=[
                {"company": "Stark Industries", "title": "CEO"},
                {"company": "Wayne Enterprises", "title": "Consultant"},
            ],
        )
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # 2. Run the function with the mocked dependencies
        enrich_and_update_engagers(unenriched_df, mock_cursor, mock_conn)

        # 3. Assert that the dependencies were called correctly
        assert mock_scrape_company.call_count == 2
        mock_conn.commit.assert_called() # Ensure the transaction is committed

        # 4. Verify the database was updated with the correct data
        # This is the most critical assertion. We check that the `execute`
        # method was called with the correct SQL and data for both engagers.
        update_calls = mock_cursor.execute.call_args_list
        assert len(update_calls) == 2

        # Check the first call
        args1, _ = update_calls[0]
        assert "UPDATE linkedin_engagers" in args1[0]
        assert args1[1] == ("Stark Industries", "CEO", "https://linkedin.com/in/person1")

        # Check the second call
        args2, _ = update_calls[1]
        assert "UPDATE linkedin_engagers" in args2[0]
        assert args2[1] == ("Wayne Enterprises", "Consultant", "https://linkedin.com/in/person2")

    ```

### Step 1: Refactor the Code for Testability and Atomicity

The test above won't pass yet because the original `enrich_companies.py` script is not structured in a way that allows for this kind of testing. We need to refactor it.

**Original `enrich_companies.py` (Simplified):**

```python
# ... (imports and setup) ...

def main():
    # ... (connects to db, gets data) ...
    for _, row in df.iterrows():
        linkedin_url = row["linkedin_url"]
        try:
            # API call inside the loop
            result = scrape_company(linkedin_url)
            company = result.get("company")
            title = result.get("title")

            # DB update inside the loop (NOT ATOMIC)
            cursor.execute(
                "UPDATE linkedin_engagers SET ...",
                (company, title, linkedin_url)
            )
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to enrich {linkedin_url}: {str(e)}")
    # ... (close connection) ...
```

**Refactored `enrich_companies.py`:**

We will break the logic into smaller, focused functions and use a bulk-update method for an atomic database write.

```python
# include/enrich_companies.py (New, Refactored Version)
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from utils import scrape_company, get_db_connection, get_db_cursor

# ... (logging configuration remains the same) ...
logger = logging.getLogger(__name__)

# (ensure_columns_exist, ensure_companies_table_exists, migrate_companies_from_engagers remain the same)

def get_unenriched_engagers(cursor):
    # ... (this function is already good, no changes needed) ...
    query = """
        SELECT DISTINCT linkedin_url
        FROM linkedin_engagers
        WHERE (company IS NULL OR company = '' OR title IS NULL OR title = '')
        AND linkedin_url IS NOT NULL AND linkedin_url != ''
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=["linkedin_url"])

def enrich_engagers_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DataFrame of unenriched engagers and returns a DataFrame with
    enriched company and title information. This is PURE LOGIC, no I/O.
    """
    enriched_results = []
    for _, row in df.iterrows():
        linkedin_url = row["linkedin_url"]
        try:
            logger.info(f"Enriching {linkedin_url} ...")
            result = scrape_company(linkedin_url)
            enriched_results.append({
                "linkedin_url": linkedin_url,
                "company": result.get("company"),
                "title": result.get("title"),
            })
        except Exception as e:
            logger.error(f"Failed to enrich {linkedin_url}: {str(e)}")
            # Optionally add a row with nulls to prevent re-processing
            enriched_results.append({
                "linkedin_url": linkedin_url,
                "company": "enrichment_failed",
                "title": "enrichment_failed",
            })
    return pd.DataFrame(enriched_results)

def update_engagers_in_db(cursor, conn, enriched_df: pd.DataFrame):
    """
    Updates the database with enriched data in a single, atomic transaction.
    """
    if enriched_df.empty:
        logger.info("No data to update in the database.")
        return

    # Prepare data for bulk update. It must be a list of tuples.
    update_data = [
        (row["company"], row["title"], row["linkedin_url"])
        for _, row in enriched_df.iterrows()
    ]

    # This is the atomic bulk update command.
    update_query = """
        UPDATE linkedin_engagers
        SET company = data.company, title = data.title
        FROM (VALUES %s) AS data(company, title, linkedin_url)
        WHERE linkedin_engagers.linkedin_url = data.linkedin_url;
    """
    try:
        logger.info(f"Starting bulk update for {len(update_data)} records...")
        execute_values(cursor, update_query, update_data)
        conn.commit()
        logger.info("Bulk update completed successfully.")
    except Exception as e:
        logger.error(f"Database bulk update failed: {e}")
        conn.rollback() # Rollback the transaction on failure
        raise

def main():
    logger.info("Starting company/title enrichment and company migration...")
    conn = get_db_connection()
    cursor = get_db_cursor(conn)
    try:
        # (Schema and migration calls remain the same)
        ensure_columns_exist(cursor)
        ensure_companies_table_exists(cursor)
        conn.commit()
        migrate_companies_from_engagers(cursor, conn)

        # --- Refactored Logic Flow ---
        # 1. Read from DB
        unenriched_df = get_unenriched_engagers(cursor)
        logger.info(f"Found {len(unenriched_df)} engagers to enrich.")

        if not unenriched_df.empty:
            # 2. Enrich data in memory (no DB writes here)
            enriched_df = enrich_engagers_data(unenriched_df)
            # 3. Write to DB in a single atomic transaction
            update_engagers_in_db(cursor, conn, enriched_df)
        else:
            logger.info("No engagers need enrichment.")

    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()

# Note: The original `enrich_and_update_engagers` function should be removed
# or refactored into the new structure. For the test to pass, you might need
# a temporary function that mimics the old structure but calls the new one.
# The best approach is to adapt the test to call the new, cleaner functions.
```

### Step 2: Verify and Deploy

1.  **Run the test**:
    From your terminal, run `pytest`. Your new test for `test_enrich_companies.py` should now pass, proving that your refactored code works exactly as the old code did, but with a much better design.

2.  **Run the DAG**:
    With the test passing, you can confidently deploy this change. Trigger the DAG manually and monitor the logs. The script will now be more resilient. If it fails during the enrichment API calls, the database won't be left in a half-updated state.

---

## 4. Applying This Pattern to Other Scripts

You can now use this `Test -> Refactor -> Verify` workflow for other scripts:

*   **`gs_sql.py`**: The logic that reads from Google Sheets and inserts into PostgreSQL can be refactored. The database insert should be a single, atomic `execute_values` call with `ON CONFLICT DO UPDATE` logic to ensure idempotency.
*   **`enrich_posts.py`**: Similar to the company enrichment, separate the scraping/enrichment logic from the database update logic. Perform a bulk update at the end.
*   **`sql_hs.py`**: The logic that reads from your database and pushes to HubSpot can be tested by mocking both the database read and the HubSpot API client.

By following this playbook, you will systematically and safely improve the quality of your pipeline, making it more robust, maintainable, and professional.
