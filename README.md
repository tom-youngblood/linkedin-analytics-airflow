# Organic Social Pipeline v2 

**Automate, Integrate, and Scale LinkedIn Lead Generation**

---

## ✨ What is This?
A robust, fully-automated pipeline that:
- **Syncs LinkedIn post data from Google Sheets** (for accessibilty of non-technical users)
- **Scrapes post engagement data** using Apify, not your own LinkedIn account and cookies--no risk!
- **Stores and deduplicates everything in PostgreSQL**
- **Pushes unique leads to HubSpot**
- **Runs itself daily via GitHub Actions**-—no manual intervention required!
- **Uses AWS RDS for reliable, stateful automation**

---

## 🏗️ Architecture & Flow

```mermaid
graph TD;
    A[Google Sheets] -->|Sync| B(Sync Script: gs_sql.py)
    B -->|Update| C[PostgreSQL DB]
    C -->|Select| D(Scrape Script: scrape.py)
    D -->|Scrape & Store| C
    C -->|Find New Leads| E(HubSpot Sync: sql_hs.py)
    E -->|Push| F[HubSpot]
```

- **Google Sheets**: Source of truth for LinkedIn post URLs/names
- **Python Scripts**: Modular, reliable, and easy to extend
- **PostgreSQL**: Robust, scalable database hosted on AWS RDS
- **HubSpot**: Receives only new, unique leads
- **GitHub Actions**: Schedules and runs the pipeline

---

## 🛠️ Key Features
- **No Duplicates**: Deduplication at every step
- **Automatic Scheduling**: Runs daily, M–F, at a random time (UTC 9–17)
- **Stateful**: Database hosted on AWS RDS—no data loss
- **Robust Logging**: All scripts log to timestamped files for easy debugging
- **Non-Technical Friendly**: Creative team updates a Google Sheet, pipeline does the rest
- **Easy to Extend**: Modular scripts for each stage

---

## 📝 SQL Schema
```sql
CREATE TABLE IF NOT EXISTS linkedin_posts (
    id SERIAL PRIMARY KEY,
    post_url TEXT UNIQUE,
    post_name TEXT,
    last_scraped_at TIMESTAMP,
    scrape_count INTEGER DEFAULT 0,
    total_reactions INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS linkedin_posts_scrapes (
    id SERIAL PRIMARY KEY,
    post_url TEXT,
    ran_at TIMESTAMP,
    reactions_count INTEGER,
    cost REAL,
    status TEXT,
    FOREIGN KEY (post_url) REFERENCES linkedin_posts(post_url)
);

CREATE TABLE IF NOT EXISTS linkedin_engagers (
    id SERIAL PRIMARY KEY,
    scrape_id INTEGER,
    linkedin_url TEXT,
    name TEXT,
    headline TEXT,
    engagement_type TEXT,
    post_url TEXT,
    pushed_to_hubspot BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (scrape_id) REFERENCES linkedin_posts_scrapes(id),
    UNIQUE(linkedin_url, post_url)
);
```

---

## 🧑‍💻 Scraping Algorithm
```mermaid
flowchart TD
    A["Start Scraping Cycle"] --> B{"Are there posts never scraped?"}
    B -- "Yes" --> C["Scrape newest never-scraped post(s)"]
    B -- "No" --> D{"Are there posts with < 5 scrapes AND last_scraped_at < now - 2 days?"}
    D -- "Yes" --> E["Scrape newest eligible post(s)"]
    D -- "No" --> F["Wait until cooldown expires or new posts arrive"]
    C --> G["Update last_scraped_at and scrape_count"]
    E --> G
    F --> H["End Cycle"]
    G --> H
    H -->|"Next scheduled run"| A
```

## ⚡️ How It Works (Workflow)
1. **Google Sheets Sync**: `gs_sql.py` pulls new posts/names, deduplicates, and updates the DB
2. **Scrape**: `scrape.py` scrapes LinkedIn post engagement, stores results in DB
3. **HubSpot Sync**: `sql_hs.py` finds new leads and pushes them to HubSpot
4. **Logging**: Each script writes detailed logs to `scripts/logs/`

---

## 🚀 Quickstart
1. **Clone the repo**
2. **Install dependencies**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Set up your environment variables**
   ```
   DB_HOST=your_rds_host
   DB_PORT=5432
   DB_NAME=your_db_name
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   SERVICE_ACCOUNT_KEY=your_google_sheets_key
   APIFY_API_KEY=your_apify_key
   HUBSPOT_API_KEY=your_hubspot_key
   ```
4. **Run locally (for testing)**
   ```bash
   cd scripts
   python gs_sql.py
   python scrape.py
   python sql_hs.py
   ```
5. **Automate with GitHub Actions**
   - Add your secrets in the repo settings
   - The workflow will run automatically

---

## 🧩 Scripts Overview
- **gs_sql.py**: Syncs Google Sheets → PostgreSQL
- **scrape.py**: Scrapes LinkedIn post engagement → PostgreSQL
- **sql_hs.py**: Pushes new leads from PostgreSQL → HubSpot
- **utils.py**: Shared helpers for scraping, ingestion, and HubSpot API

---

## 🛡️ Reliability & Extensibility
- **Risk free LinkedIn Scraping**: Uses external accounts, cookies, and IPs
- **Database hosted on AWS RDS**: Reliable and scalable
- **Logs for every run**: Debug and audit with ease
- **Easy to add new sources or destinations**: Just add a script and update the workflow

---

## 👩‍💻 For Non-Technical Users
- Just update the Google Sheet—no code, no problem!
- All syncing, scraping, and pushing is fully automated

---

## 📈 Why Use This Pipeline?
- **Save time**: No more manual copy-paste or lead uploads
- **Reduce errors**: Automation means fewer mistakes
- **Scale easily**: Add more posts, more scrapes, or more destinations
- **Audit everything**: Logs and database history are always available
---
