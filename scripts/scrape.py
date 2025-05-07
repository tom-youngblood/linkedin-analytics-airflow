# Query database to get top 3 posts to scrape: select P.post_name, P.post_url, S.ran_at, s_recent (window function to get the date)
# Scrape top three posts with APIfy
# Update scrapes table
# Send posts to hubspot

from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd
import utils

# Connect to SQLite DB
conn = sqlite3.connect('../data/post_scrapes.db')
cursor = conn.cursor()

# Query: Scrape most recent posts 3 times
cursor.execute("""
SELECT p.post_url
FROM posts p
WHERE p.last_scraped_at IS NULL OR p.scrape_count < 3
ORDER BY rowid DESC
LIMIT 3
""")

# Convert results to dataframe
df = pd.DataFrame(cursor.fetchall(), columns=["link"])
print(df)

# Iterate through posts
#for _, row in df.iterrows():
    # Scrape posts
    #utils.scrape_post(row["link"])

    # Ingest postss to SQL DB
    #utils.ingest_scrape(conn, cursor, )


conn.commit()
conn.close()
