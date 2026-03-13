import os
import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

load_dotenv()

TRACKER_TABLE_NAME = os.environ.get('TRACKER_TABLE_NAME')
GSI_NAME = os.environ.get('GSI_NAME')
REGION = os.environ.get('AWS_DEFAULT_REGION') 

try:
  dynamodb = boto3.resource('dynamodb', region_name=REGION)
  tracker_table = dynamodb.Table(TRACKER_TABLE_NAME)
except Exception as e:
  print(f"Error initializing boto3 for DynamoDB: {e}")


def search_pending_trips():
  print("# Searching pending trips")
  try:
    response = tracker_table.query(
      IndexName=GSI_NAME,
      KeyConditionExpression=Key('status').eq('PENDING')
    )
    pending_trips = response.get('Items', [])

    if not pending_trips:
      print("  > No pending trips to process. Going back to sleep.")
    else:
      print(f" > Found {len(pending_trips)} PENDING trips")

    return pending_trips
  except Exception as e:
    print(f"❌ Error searching DynamoDB: {e}")
    return []