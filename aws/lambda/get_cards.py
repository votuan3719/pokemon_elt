import json
import boto3
import os
import time
import requests
from time import sleep

API_KEY = os.environ.get('API_KEY')
BUCKET_NAME = os.environ.get('S3_BUCKET')

s3_client = boto3.client('s3')

def get_card_prices(api_key, card_ids):
    url = 'https://www.pokemonpricetracker.com/api/cards/bulk-history'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    payload = {
        'cardIds': card_ids,
        'type': 'all',
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return {'cards': []} 

def lambda_handler(event, context):
    set_id = event['setId']
    to_process = event.get('toProcess', []) 
    
    if not to_process:
        return {
            "setId": set_id,
            "cardsProcessed": 0,
            "claimCheck": None,
            "status": "No cards to process."
        }
    
    # Retry configuration
    max_retries = 5
    retry_delay = 2  # seconds
    
    try:
        all_cards_data = []
        retry_count = 0
        
        while retry_count < max_retries:
            all_cards_data = []
            
            # Process all batches
            for batch in to_process:
                price_info = get_card_prices(API_KEY, batch)
                cards = price_info.get('cards')
                if isinstance(cards, list):
                    all_cards_data.extend(cards)
            
            # If we got data, break out of retry loop
            if all_cards_data:
                print(f"Successfully retrieved {len(all_cards_data)} cards")
                break
            
            # No data received, retry if we haven't hit max retries
            retry_count += 1
            if retry_count < max_retries:
                print(f"No card data received. Retry {retry_count}/{max_retries} after {retry_delay} seconds...")
                sleep(retry_delay)
                # Optional: exponential backoff
                retry_delay *= 2
            else:
                print(f"Failed to retrieve card data after {max_retries} attempts")

        # Check if we still have no data after all retries
        if not all_cards_data:
            return {
                "setId": set_id,
                "cardsProcessed": 0,
                "claimCheck": None,
                "status": f"API returned no card data after {max_retries} retries."
            }

        date = time.strftime("%Y-%m-%d")
        file_key = f'prices/{date}/{set_id}.json'
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(all_cards_data)
        )

        claim_check = {
            'bucket': BUCKET_NAME,
            'key': file_key
        }
        
        return {
            "setId": set_id,
            "cardsProcessed": len(all_cards_data),
            "claimCheck": claim_check,
            "status": f"Success after {retry_count} retries" if retry_count > 0 else "Success"
        }
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e