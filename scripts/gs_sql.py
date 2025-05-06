import sqlite3
import pandas as pd
import gspread
from dotenv import load_dotenv
import json
import os

# Load Environment variables
load_dotenv()
gspread_credentials = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])  


# Connect to Google Sheets
gc = gspread.service_account_from_dict(gspread_credentials)
links = pd.DataFrame(gc.open("Organic Social Dashboard").worksheet('LI Links').get_all_values(), columns=['link', 'post', 'id'])

print(links)
"""
# Connect to SQLite database
conn = sqlite3.connect('gs_data.db')

# Read data into a pandas DataFrame
df = pd.read_sql_query('SELECT * FROM gs_data', conn)

# Close SQLite connection
conn.close()
"""
