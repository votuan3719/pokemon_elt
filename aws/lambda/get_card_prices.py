import os
import boto3
import requests
import json
import time
from time import sleep

API_KEY = os.environ.get('API_KEY')
BUCKET_NAME = os.environ.get('S3_BUCKET')

s3_client = boto3.client('s3')

def get_card_prices(card_ids):
    url = 'https://www.pokemonpricetracker.com/api/cards/bulk-history'
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    payload = {
        'cardIds': card_ids,
        'type': 'all',
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json() 

        return data
    except requests.exceptions.RequestException as e:
        return None

def lambda_handler(event, context):
    set_id = event['setId']

    cards = event['toProcess']
    if not cards:
        return {
            "setId": set_id,
            "cardsProcessed": 0,
            "claimCheck": None,
            "status": "No cards to process."
        }
    
    max_retries = 5
    retry_delay = 2
    try:
        card_prices = []
        
        retry_count = 0
        while retry_count < max_retries:
            for batch in cards:

                price_data = get_card_prices(batch)
                if price_data:

                    price_info = price_data['cards']
                    if isinstance(price_info, list):
                        card_prices.extend(price_info)
            
            if card_prices:
                break
            
            if retry_count < max_retries:
                retry_count += 1
                sleep(retry_delay)
                retry_delay *= 2

        if not card_prices:
            return {
                "setId": set_id,
                "cardsProcessed": 0,
                "claimCheck": None,
                "status": "API returned no card data."
            }

        curr_date = time.strftime("%Y-%m-%d")
        file_path = f'prices/{curr_date}/{set_id}.json'
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_path,
            Body=json.dumps(card_prices)
        )

        return {
            "setId": set_id,
            "cardsProcessed": len(card_prices),
            "claimCheck": {
                'bucket': BUCKET_NAME,
                'key': file_path
            },
            "status": f"Success after {retry_count} retries" if retry_count > 0 else "Success"
        }    
    except Exception as e:
        raise e