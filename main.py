import asyncio
from hubspot_client import fetch_deals, process_deals
from duplicate_finder import find_potential_duplicates
from csv_exporter import export_to_csv

async def main():
  # Get user input for the number of deals to fetch
  while True:
    try:
      num_deals = int(input("Enter the number of deals to fetch (1-1000, or enter 0 for all available deals): "))
      if 0 <= num_deals <= 1000:
        break
      else:
        print("Please enter a number between 0 and 1000.")
    except ValueError:
      print("Please enter a valid number.")

  if num_deals == 0:
    print("Fetching all available deals from HubSpot...")
    num_deals = 10000  # Set a high number to fetch all deals
  else:
    print(f"Fetching {num_deals} deals from HubSpot...")

  deals = await fetch_deals(num_deals)

  print(f"Successfully fetched {len(deals)} deals.")
  print("Processing deals...")
  processed_deals = await process_deals(deals)

  print("Identifying potential duplicates...")
  deals_with_duplicates = await find_potential_duplicates(processed_deals)

  print("Exporting results to CSV...")
  export_to_csv(deals_with_duplicates)

  print("Done! Check deal_duplicates.csv for results.")

if __name__ == "__main__":
  asyncio.run(main())