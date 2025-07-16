import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import utils
import airflow_utils
import openai
from openai import OpenAI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/enrich_hubspot_contacts_{datetime.now().strftime("%Y%m%d")}.log'),
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

def main():
    # Load Environment variables (for backward compatibility)
    logger.info("Loading environment variables")
    load_dotenv()

    # Connect to PostgreSQL database
    logger.info("Connecting to PostgreSQL database")
    try:
        conn = utils.get_db_connection()
        cursor = utils.get_db_cursor(conn)
        logger.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
        raise

    # 1. Get all local engagers with their full details in one query
    local_engagers_query = """
        SELECT
            e.linkedin_url,
            e.name,
            e.headline,
            MIN(p.post_name) as post_name
        FROM linkedin_engagers_by_post e
        JOIN linkedin_posts p ON e.post_url = p.post_url
        WHERE e.linkedin_url IS NOT NULL AND e.linkedin_url != ''
        GROUP BY e.linkedin_url, e.name, e.headline
    """
    local_engagers = utils.log_query_results(cursor, "Local engagers query", local_engagers_query)
    local_engagers_df = pd.DataFrame(local_engagers, columns=["linkedin_url", "name", "headline", "post_name"])
    logger.info(f"Got {len(local_engagers_df)} total local engagers for enrichment.")

    # 2. Fetch HubSpot contacts with all required properties
    hs_api_key = airflow_utils.get_required_env_var("HUBSPOT_API_KEY")
    list_id = airflow_utils.get_optional_env_var("HUBSPOT_LIST_ID", "246")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all"
    properties = ["hs_linkedin_url", "company", "jobtitle", "engager_audience", "engager_bucketed_position"]
    hubspot_contacts_df = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties)
    logger.info(f"Fetched {len(hubspot_contacts_df)} HubSpot contacts from list {list_id} for enrichment.")

    # Clean URLs for matching
    local_engagers_df['linkedin_url_clean'] = local_engagers_df['linkedin_url'].str.strip().str.lower()
    hubspot_contacts_df['hs_linkedin_url_clean'] = hubspot_contacts_df['hs_linkedin_url'].str.strip().str.lower()

    # 3. Identify new contacts to upload (for enrichment purposes)
    existing_urls = set(hubspot_contacts_df['hs_linkedin_url_clean'].dropna().unique())
    engagers_to_upload = local_engagers_df[~local_engagers_df['linkedin_url_clean'].isin(existing_urls)]
    engagers_to_upload = engagers_to_upload[engagers_to_upload['linkedin_url'].str.contains('/in/', na=False)]
    
    # Upload new contacts to HubSpot if any exist
    if not engagers_to_upload.empty:
        engagers_to_upload["firstname"] = engagers_to_upload["name"].apply(lambda x: str(x).split()[0] if pd.notnull(x) and str(x).strip() else "")
        engagers_to_upload["lastname"] = engagers_to_upload["name"].apply(lambda x: " ".join(str(x).split()[1:]) if pd.notnull(x) and len(str(x).split()) > 1 else "")
        
        engagers_to_upload_properties = {
            'linkedin_url': 'hs_linkedin_url',
            'headline': 'phantombuster_linkedin_headline',
            'post_name': 'post_name',
            'firstname': 'firstname',
            'lastname': 'lastname'
        }
        logger.info(f"Uploading {len(engagers_to_upload)} new contacts to HubSpot before enrichment...")
        utils.hubspot_push_contacts_to_list(hs_api_key, engagers_to_upload, engagers_to_upload_properties)
        
        # Re-fetch HubSpot contacts to include newly uploaded ones
        hubspot_contacts_df = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties)
        logger.info(f"Re-fetched {len(hubspot_contacts_df)} HubSpot contacts after upload.")

    # 4. Enrich all contacts in the list
    all_contacts_df = pd.concat([hubspot_contacts_df, engagers_to_upload]) if not engagers_to_upload.empty else hubspot_contacts_df

    # Only enrich contacts missing at least one of the target fields
    fields_to_check = ["company", "jobtitle", "engager_audience", "engager_bucketed_position"]
    def needs_enrichment(row):
        return any(pd.isnull(row[field]) or row[field] == "" for field in fields_to_check)
    contacts_to_enrich = all_contacts_df[all_contacts_df.apply(needs_enrichment, axis=1)]
    if len(contacts_to_enrich) > 100:
        logger.info(f"Limiting enrichment to 100 out of {len(contacts_to_enrich)} contacts needing enrichment.")
        contacts_to_enrich = contacts_to_enrich.head(100)
    else:
        logger.info(f"Enriching {len(contacts_to_enrich)} contacts in HubSpot list {list_id} that are missing at least one target field.")

    update_count = 0
    for _, row in contacts_to_enrich.iterrows():
        contact_id = row.get("vid") or row.get("id")
        linkedin_url = row.get("hs_linkedin_url") or row.get("linkedin_url")

        # Normalize contact_id to string without decimals
        if contact_id is not None:
            if isinstance(contact_id, float) and contact_id.is_integer():
                contact_id = str(int(contact_id))
            else:
                contact_id = str(contact_id)
        else:
            continue
        
        if not linkedin_url:
            continue

        # Enrich company and title
        company_info = utils.scrape_company(linkedin_url)
        company = company_info.get("company")
        title = company_info.get("title")

        # Enrich audience
        audience, position = classify_audience_with_openai(company, title, row.get("name"), row.get("headline"))

        update_fields = {}
        if pd.notnull(company) and company and company != row.get("company"):
            update_fields["company"] = company
        if pd.notnull(title) and title and title != row.get("jobtitle"):
            update_fields["jobtitle"] = title
        if pd.notnull(audience) and audience and audience != row.get("engager_audience"):
            update_fields["engager_audience"] = audience
        if pd.notnull(position) and position and position != row.get("engager_bucketed_position"):
            update_fields["engager_bucketed_position"] = position

        if update_fields and contact_id:
            utils.hubspot_update_contact_fields(hs_api_key, contact_id, update_fields)
            update_count += 1
            
    logger.info(f"Updated {update_count} HubSpot contacts in list {list_id} with new enrichment info.")

    cursor.close()
    conn.close()
    logger.info("HubSpot contact enrichment completed successfully.")

if __name__ == "__main__":
    main() 