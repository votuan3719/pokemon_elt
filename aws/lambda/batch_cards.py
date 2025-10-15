import os
import boto3
import json
import pandas as pd
from itertools import islice
import datetime 

BUCKET_NAME = os.environ.get('S3_BUCKET')
FILE_PATH = os.environ.get('S3_FILE_PATH')

s3 = boto3.client('s3')


def batches(set, amount):
    it = iter(set)
    while True:
        batch = list(islice(it, amount))
        if not batch:
            break
        yield batch

def lambda_handler(event, context):
    plan = []

    request = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_PATH)
    response = request['Body'].read()

    cards_df = pd.read_csv(response)
    for set_id, group in cards_df.groupby('set_id'):

        card_ids = list(group['id'])
        info = {
            'setId': set_id,
            'toProcess': []
        }
        for batch in batches(card_ids, 50):
            info['toProcess'].append(batch)
        
        plan.append(info)

    curr_date = datetime.datetime.now().strftime("%Y-%m-%d")
    plan_path = f'plan/{curr_date}/plan.json'
    try:
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=plan_path,
            Body=json.dumps(plan),
            ContentType='application/json'
        )
        return {
            'status': 'Success',
            'claimCheck': {
                'bucket': BUCKET_NAME,
                'key': plan_path
            }
        }
    except Exception as e:
        return {
            'status': 'Failed',
            'claimCheck': None
        }