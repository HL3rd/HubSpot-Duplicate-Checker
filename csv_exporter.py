import csv

def export_to_csv(deals, filename="deal_duplicates.csv"):
  with open(filename, 'w', newline='') as csvfile:
    fieldnames = ['ID', 'Name', 'Company', 'Close Date', 'Amount', 'Stage', 'Owner', 'Potential %', 'Top Duplicate Deal', 'Top Duplicate Deal - Company', 'Potential Duplicates']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for deal in deals:
      top_duplicate = max(deal['potential_duplicates'], key=lambda x: x['percentage']) if deal['potential_duplicates'] else None
      writer.writerow({
        'ID': deal['id'],
        'Name': deal['name'],
        'Company': get_company_name(deal),
        'Close Date': deal['close_date'],
        'Amount': deal['amount'],
        'Stage': deal.get('stage', 'Unknown'),
        'Owner': deal['owner_email'],
        'Potential %': deal['duplicate_percentage'],
        'Top Duplicate Deal': top_duplicate['deal'] if top_duplicate else '',
        'Top Duplicate Deal - Company': top_duplicate['company'] if top_duplicate else '',
        'Potential Duplicates': format_potential_duplicates(deal['potential_duplicates'])
      })

def get_company_name(deal):
  if deal['associatedCompanies']:
    return deal['associatedCompanies'][0]['company_name']
  return "Unknown"

def format_potential_duplicates(potential_duplicates):
  return '; '.join([f"{d['deal']} ({d['company']}) ({d['percentage']}%): {d['explanation']}" for d in potential_duplicates])