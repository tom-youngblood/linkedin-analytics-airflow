import sqlite3
import pandas as pd
import gspread
from dotenv import load_dotenv
import json
import base64
import os

# Load Environment variables
load_dotenv()

# Get the encoded key from environment variable
encoded_key = str(os.getenv("SERVICE_ACCOUNT_KEY"))[2:-1]

# Decode the key
gspread_credentials = json.loads(base64.b64decode(encoded_key).decode('utf-8'))

# Connect to Google Sheets
gc = gspread.service_account_from_dict(gspread_credentials)
links = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])

# Connect to SQLite database
conn = sqlite3.connect('../data/post_scrapes.db')
cursor = conn.cursor()

# Create posts table
cursor.execute("""
CREATE TABLE IF NOT EXISTS posts (
    post_url TEXT PRIMARY KEY,
    post_name TEXT,
    last_scraped_at TEXT,
    scrape_count INTEGER,
    total_reactions INTEGER
)
""")

# Create scrapes table
cursor.execute("""
CREATE TABLE IF NOT EXISTS scrapes (
    scrape_id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_url TEXT,
    ran_at TEXT,
    reactions_count INTEGER,
    cost REAL,
    status TEXT,
    FOREIGN KEY (post_url) REFERENCES posts(post_url)
)
""")

# Ingest posts and post_name from DF
for _, row in links.iterrows():
    cursor.execute("""
        INSERT INTO posts (post_url, post_name, last_scraped_at, scrape_count, total_reactions)
        VALUES (?, ?, NULL, 0, 0)
        ON CONFLICT(post_url) DO UPDATE SET post_name=excluded.post_name
    """, (row['link'], row['post']))
conn.commit()

# Select * from posts
conn.close()




