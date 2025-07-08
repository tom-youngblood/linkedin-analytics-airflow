import utils
import airflow_utils
import pandas as pd

def main():
    hs_api_key = airflow_utils.get_required_env_var("HUBSPOT_API_KEY")
    list_id = airflow_utils.get_optional_env_var("HUBSPOT_LIST_ID", "246")
    url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}/contacts/all?property=hs_linkedin_url&property=company&property=jobtitle"
    properties = ["hs_linkedin_url", "company", "jobtitle"]
    hubspot_contacts = utils.hubspot_fetch_list_contacts(hs_api_key, url, properties)
    print(f"Fetched {len(hubspot_contacts)} contacts from list {list_id}")

    update_count = 0
    for _, row in hubspot_contacts.iterrows():
        contact_id = row.get("vid") or row.get("id")
        linkedin_url = row.get("hs_linkedin_url")
        company = row.get("company")
        jobtitle = row.get("jobtitle")

        # Only enrich if linkedin_url exists and company or jobtitle is missing
        if linkedin_url and (not company or pd.isnull(company) or not jobtitle or pd.isnull(jobtitle)):
            print(f"Enriching {linkedin_url} (HubSpot ID: {contact_id}) ...")
            result = utils.scrape_company(linkedin_url)
            scraped_company = result.get("company")
            scraped_title = result.get("title")

            # Only update if we got at least one new value
            if (scraped_company and (not company or pd.isnull(company))) or (scraped_title and (not jobtitle or pd.isnull(jobtitle))):
                utils.hubspot_update_contact(
                    hs_api_key,
                    contact_id,
                    scraped_company if scraped_company else company,
                    scraped_title if scraped_title else jobtitle
                )
                update_count += 1
                print(f"Updated HubSpot contact {contact_id} with company='{scraped_company}', jobtitle='{scraped_title}'")
            else:
                print(f"No new data found for {linkedin_url}")
    print(f"Updated {update_count} HubSpot contacts in list {list_id} with scraped company/jobtitle.")

if __name__ == "__main__":
    main() 