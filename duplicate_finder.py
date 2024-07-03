import os
import re
import asyncio
import time
from openai import AsyncOpenAI

client = AsyncOpenAI(api_key=os.environ['OPENAI_API_KEY'])

# Rate limiting parameters for OpenAI
RATE_LIMIT_CALLS = 50  # Adjust this based on your OpenAI plan
RATE_LIMIT_PERIOD = 60  # 1 minute
last_call_time = 0
call_count = 0

async def rate_limit():
  global last_call_time, call_count
  current_time = time.time()
  if current_time - last_call_time < RATE_LIMIT_PERIOD:
    call_count += 1
    if call_count >= RATE_LIMIT_CALLS:
      sleep_time = RATE_LIMIT_PERIOD - (current_time - last_call_time)
      if sleep_time > 0:
        print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
        await asyncio.sleep(sleep_time)
      last_call_time = time.time()
      call_count = 0
  else:
    last_call_time = current_time
    call_count = 1

async def find_potential_duplicates(deals):
  tasks = []
  for i, deal in enumerate(deals):
    for j, other_deal in enumerate(deals):
      if i != j:
        tasks.append(compare_deals(deal, other_deal))

  results = await asyncio.gather(*tasks)

  for deal, duplicates in zip(deals, [results[i:i+len(deals)-1] for i in range(0, len(results), len(deals)-1)]):
    potential_duplicates = [dup for dup in duplicates if dup['percentage'] > 50]
    deal['potential_duplicates'] = potential_duplicates
    deal['duplicate_percentage'] = max([d['percentage'] for d in potential_duplicates]) if potential_duplicates else 0

  return deals

async def compare_deals(deal, other_deal):
  company_name = get_company_name(deal)
  other_company_name = get_company_name(other_deal)

  # Pre-check to avoid unnecessary API calls
  if company_name != other_company_name:
    return {
      "deal": other_deal['name'],
      "company": other_company_name,
      "percentage": 0,
      "explanation": "Different companies"
    }

  if deal['close_date'] != other_deal['close_date'] and deal['amount'] != other_deal['amount']:
    return {
      "deal": other_deal['name'],
      "company": other_company_name,
      "percentage": 0,
      "explanation": "Neither close date nor amount match"
    }

  prompt = f"""
  Deal 1:
  - Name: {deal['name']}
  - Company: {company_name}
  - Close Date: {deal['close_date']}
  - Amount: {deal['amount']}
  - Owner: {deal['owner_email']}

  Deal 2:
  - Name: {other_deal['name']}
  - Company: {other_company_name}
  - Close Date: {other_deal['close_date']}
  - Amount: {other_deal['amount']}
  - Owner: {other_deal['owner_email']}

  Are these deals potential duplicates?

  A deal is considered a duplicate if and only if ALL of these conditions are true:
  1. The company names are exactly the same AND
  2. EITHER the close dates are exactly the same OR the amounts are exactly the same

  If these conditions are not ALL met, the deals are not duplicates.

  Respond ONLY in the following format - no other format is a valid response from you:
  Percentage: [100 if all conditions are met, 0 if not]
  Explanation: [Brief explanation of why it is or isn't a duplicate based on the specific criteria]
  """

  max_retries = 5
  for attempt in range(max_retries):
    try:
      await rate_limit()
      completion = await client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
          {
            "role": "system",
            "content": """
            You are a strict duplicate deal identifier. You MUST follow the given criteria exactly.
            Always respond in the exact format specified.
            """
          },
          {
            "role": "user", 
            "content": prompt
          }
        ]
      )

      result = completion.choices[0].message.content.strip()
      percentage, explanation = parse_response(result)

      if percentage > 50:
        print('Comparing', deal['name'], 'from', company_name, 'and', '\n', other_deal['name'], 'from', other_company_name, '\n', result, '\n')

      return {
        "deal": other_deal['name'],
        "company": other_company_name,
        "percentage": percentage,
        "explanation": explanation
      }
    except Exception as e:
      if hasattr(e, 'status_code') and e.status_code == 429:
        if attempt < max_retries - 1:
          wait_time = (2 ** attempt) + 1  # exponential backoff
          print(f"Rate limit error. Retrying in {wait_time} seconds...")
          await asyncio.sleep(wait_time)
        else:
          print(f"Max retries reached. Skipping this comparison.")
          return {
            "deal": other_deal['name'],
            "company": other_company_name,
            "percentage": 0,
            "explanation": "Error: Unable to compare due to rate limiting"
          }
      else:
        print(f"Error comparing deals: {e}")
        return {
          "deal": other_deal['name'],
          "company": other_company_name,
          "percentage": 0,
          "explanation": f"Error: {str(e)}"
        }

def get_company_name(deal):
  if deal['associatedCompanies']:
    return deal['associatedCompanies'][0]['company_name']
  return "Unknown"

def parse_response(response):
  percentage_match = re.search(r'Percentage:\s*(\d+)', response)
  explanation_match = re.search(r'Explanation:\s*(.+)', response)

  percentage = int(percentage_match.group(1)) if percentage_match else 0
  explanation = explanation_match.group(1) if explanation_match else "No explanation provided"

  return percentage, explanation