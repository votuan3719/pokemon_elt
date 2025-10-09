import os
import json
import boto3
import datetime 
import pandas as pd
from itertools import islice

BUCKET_NAME = os.environ.get('S3_BUCKET')
FILE_KEY = os.environ.get('S3_FILE_KEY')

s3 = boto3.client('s3')

from itertools import islice

def chunks(set, size):
    it = iter(set)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch

def lambda_handler(event, context):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    pokemon_df = pd.read_csv(response['Body'])

    plan = []
    for set_id, group in pokemon_df.groupby('set_id'):
        card_ids = list(group['id'])
    
        info = {
            'setId': set_id,
            'toProcess': []
        }
        for batch in chunks(card_ids, 50):
            info['toProcess'].append(batch)
        plan.append(info)

    date = datetime.datetime.now().strftime("%Y-%m-%d")
    plan_key = f'plan/{date}/plan.json'
    try:
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=plan_key,
            Body=json.dumps(plan),
            ContentType='application/json'
        )
        return {
            'status': 'Success',
            'claimCheck': {
                'bucket': BUCKET_NAME,
                'key': plan_key
            }
        }
    except Exception as e:
        print(e)
        return {
            'status': 'Failed',
            'claimCheck': None
        }