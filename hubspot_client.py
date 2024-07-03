import os
import asyncio
import simplejson
from hubspot import HubSpot
from hubspot.crm.deals import ApiException
from urllib3.util.retry import Retry

# Initialize the HubSpot client with retry logic
hubspot_access_token = os.environ['HUBSPOT_ACCESS_TOKEN']
retry = Retry(total=5, status_forcelist=(429,))
hubspot_client = HubSpot(access_token=hubspot_access_token, retry=retry)

# Rate limiting parameters
RATE_LIMIT_CALLS = 10
RATE_LIMIT_PERIOD = 1  # in seconds
last_call_time = 0
call_count = 0

async def rate_limit():
    global last_call_time, call_count
    current_time = asyncio.get_event_loop().time()
    if current_time - last_call_time < RATE_LIMIT_PERIOD:
        call_count += 1
        if call_count >= RATE_LIMIT_CALLS:
            sleep_time = RATE_LIMIT_PERIOD - (current_time - last_call_time)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            last_call_time = asyncio.get_event_loop().time()
            call_count = 0
    else:
        last_call_time = current_time
        call_count = 1

async def api_call(func, *args, **kwargs):
    max_retries = 5
    base_delay = 1
    for attempt in range(max_retries):
        await rate_limit()
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except ApiException as e:
            if e.status == 429:  # Too Many Requests
                delay = base_delay * (2 ** attempt)
                print(f"Rate limit hit. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached")

async def fetch_deals(limit):
    all_deals = []
    after = None

    while len(all_deals) < limit:
        try:
            # Determine how many deals to fetch in this iteration
            batch_limit = min(100, limit - len(all_deals))

            # Prepare the search request
            search_request = {
                "sorts": [
                    {
                        "propertyName": "createdate",
                        "direction": "DESCENDING"
                    }
                ],
                "properties": ["dealname", "amount", "closedate", "createdate", "dealstage", "hubspot_owner_id"],
                "limit": batch_limit
            }

            if after:
                search_request["after"] = after

            # Fetch deals using the search endpoint
            deals_page = await api_call(
                hubspot_client.crm.deals.search_api.do_search,
                public_object_search_request=search_request
            )

            all_deals.extend(deals_page.results)

            if not deals_page.paging:
                break

            after = deals_page.paging.next.after

        except ApiException as e:
            print(f"Exception when fetching deals: {e}\n")
            break

    return all_deals[:limit]  # Ensure we don't return more deals than requested

async def process_deals(deals):
    processed_deals = []
    for deal in deals:
        processed_deal = await process_deal(deal)
        processed_deals.append(processed_deal)
    return processed_deals

async def process_deal(deal):
    deal_dict = deal.to_dict()

    try:
        # Get associated contacts
        contacts_page = await api_call(
            hubspot_client.crm.associations.v4.basic_api.get_page,
            'deals', deal.id, 'contacts'
        )
        contact_ids = [result.to_object_id for result in contacts_page.results]

        # Get associated companies
        companies_page = await api_call(
            hubspot_client.crm.associations.v4.basic_api.get_page,
            'deals', deal.id, 'companies'
        )
        company_ids = [result.to_object_id for result in companies_page.results]

        # Fetch owner details
        owner_email = None
        if deal.properties.get('hubspot_owner_id'):
            try:
                owner = await api_call(
                    hubspot_client.crm.owners.owners_api.get_by_id,
                    owner_id=deal.properties['hubspot_owner_id'],
                    id_property='id',
                    archived=False
                )
                owner_email = owner.properties.get('email')
            except Exception as e:
                print(f"Exception when fetching owner details for deal {deal.id}: {e}\n")
                owner_email = "Unknown"

        # Fetch full contact and company details
        contacts_batch = await api_call(
            hubspot_client.crm.contacts.batch_api.read,
            batch_read_input_simple_public_object_id={
                "properties": ['email', 'firstname', 'lastname'],
                "inputs": [{"id": id} for id in contact_ids]
            }
        ) if contact_ids else None

        companies_batch = await api_call(
            hubspot_client.crm.companies.batch_api.read,
            batch_read_input_simple_public_object_id={
                "properties": ['name', 'domain', 'hs_additional_domains', 'website'],
                "inputs": [{"id": id} for id in company_ids]
            }
        ) if company_ids else None

        # Format contact details
        formatted_contacts = []
        if contacts_batch and contacts_batch.results:
            for contact in contacts_batch.results:
                contact_dict = contact.to_dict()
                formatted_contacts.append({
                    "contact_id": int(contact_dict['id']),
                    "contact_email_addresses": [contact_dict['properties'].get('email')] if contact_dict['properties'].get('email') else [],
                    "contact_name": f"{contact_dict['properties'].get('firstname', '')} {contact_dict['properties'].get('lastname', '')}".strip()
                })

        # Format company details
        formatted_companies = []
        if companies_batch and companies_batch.results:
            for company in companies_batch.results:
                company_dict = company.to_dict()
                primary_domain = company_dict['properties'].get('domain')
                additional_domains = company_dict['properties'].get('hs_additional_domains', '').split(';') if company_dict['properties'].get('hs_additional_domains') else []
                website = company_dict['properties'].get('website')
                all_domains = list(set(filter(None, [primary_domain, website] + additional_domains)))

                formatted_companies.append({
                    "company_id": int(company_dict['id']),
                    "company_name": company_dict['properties'].get('name', ''),
                    "company_domains": all_domains
                })

        # Combine deal with its associations and owner email
        return {
            "id": deal_dict['id'],
            "name": deal_dict['properties'].get("dealname", ""),
            "amount": deal_dict['properties'].get("amount", ""),
            "close_date": deal_dict['properties'].get("closedate", ""),
            "stage": deal_dict['properties'].get("dealstage", ""),
            "owner_email": owner_email,
            "associatedContacts": formatted_contacts,
            "associatedCompanies": formatted_companies
        }

    except ApiException as e:
        print(f"Exception when processing deal {deal.id}: {e}\n")
        return deal_dict