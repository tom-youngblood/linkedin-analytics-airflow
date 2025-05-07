# Organic Social Pipeline v2

A pipeline for ingesting LinkedIn post data from Google Sheets, storing and updating it in SQLite, scraping post data, and integrating with HubSpot. Designed for automation and efficient scaling.

## Overview
This project automates the process of:
- Syncing LinkedIn post data from a Google Sheet to a local SQLite database
- Deduplicating and updating post names as needed
- Scraping post data and storing results
- Pushing unique leads to HubSpot
- Running on a schedule via GitHub Actions

## Architecture
- **Google Sheets**: Source of LinkedIn post URLs and names; easy for Creative Team to interact with and update.
- **Python Scripts**: For data ingestion, scraping, and integration
- **SQLite**: Local database for posts and scrape results
- **HubSpot**: Receives unique leads
- **GitHub Actions**: Schedules and runs the pipeline

## SQL Schema
```sql
CREATE TABLE IF NOT EXISTS posts (
    post_url TEXT PRIMARY KEY,
    post_name TEXT,
    last_scraped_at TEXT,
    scrape_count INTEGER,
    total_reactions INTEGER
);

CREATE TABLE IF NOT EXISTS scrapes (
    scrape_id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_url TEXT,
    ran_at TEXT,
    reactions_count INTEGER,
    cost REAL,
    status TEXT,
    FOREIGN KEY (post_url) REFERENCES posts(post_url)
);
```

## Google Sheets Ingestion Logic
- Loads all posts and post names from Google Sheets into a DataFrame
- Ingests all new post and post_name values into the SQLite database
- Deduplicates on `post_url` (no duplicate posts)
- Updates `post_name` if it changes for an existing `post_url`

## Setup
1. Clone the repository
2. Create a virtual environment and install dependencies
3. Set up your `.env` file with Google Sheets credentials (see scripts/encode_credentials.py for encoding instructions)
4. Run the ingestion script in `/scripts` to populate your database
5. Use the VSCode SQLite extension or DB Browser for SQLite to inspect your data

## Implementation Steps
1. Set Up the New Repository
    - Create a new GitHub repo: organic-social-pipeline-v2
    - Add a README.md describing the new architecture and flow
2. Initialize the Project Structure
    - Add directories: `/scripts`, `/data`, `/docs`
    - Add .gitignore (ignore .db files, credentials, etc.)
3. Set Up SQLite Database
    - Write a script to initialize the SQLite DB with the posts and scrapes tables
    - Optionally, add a migration or schema management script
4. Google Sheets Integration
    - Use gspread or pandas to read the LinkedIn posts list from Google Sheets
    - Write a script to sync new posts from the sheet into the posts table (see above)
5. Scraping Logic
    - Write a script to query the DB for the best post to scrape, call the Apify actor, parse/store results, and update posts
6. HubSpot Integration
    - Write a script to identify new/unique leads from the latest scrapes and push them to HubSpot via API
7. Scheduling with GitHub Actions
    - Create a workflow to run 3x daily at random times between 9-5
    - Ensure the workflow installs dependencies, runs your scripts, and persists the SQLite DB as an artifact if needed
8. Documentation
    - Document your setup, environment variables, and usage in README.md
    - Add your flowchart to /docs
9. (Optional) Error Handling & Logging
    - Add logging to your scripts
    - Handle and record failed scrapes in the scrapes table (status column)
