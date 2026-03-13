import os
import time
import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv
from utils import *
from decimal import Decimal

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

def to_decimal_list(pos):
    return [Decimal(str(pos[0])), Decimal(str(pos[1]))] if pos else None
  
def update_trip_state(batch_id, start_pos=None, current_pos=None, is_finished=False):
    update_expr_parts = []
    expr_vals = {}
    expr_names = {}
    
    if start_pos:
        city_start = get_city_name(start_pos[0], start_pos[1])
        update_expr_parts.extend(["start_pos = :pstart", "city_start = :cstart"])
        expr_vals[':pstart'] = to_decimal_list(start_pos)
        expr_vals[':cstart'] = city_start

    if current_pos:
        city_current = get_city_name(current_pos[0], current_pos[1])
        update_expr_parts.extend(["current_pos = :pcurr", "city_current = :ccurr"])
        expr_vals[':pcurr'] = to_decimal_list(current_pos)
        expr_vals[':ccurr'] = city_current

    if is_finished:
        if current_pos:
            update_expr_parts.extend(["end_pos = :pcurr", "city_end = :ccurr"])
        
        update_expr_parts.extend(["#s = :status", "expires_at = :ttl"])
        expr_vals[':status'] = 'CONSOLIDATED'
        expr_vals[':ttl'] = int(time.time()) + (60 * 60 * 24 * 7)
        expr_names['#s'] = 'status'
    else:
        update_expr_parts.append("#s = :status")
        expr_vals[':status'] = 'PENDING'
        expr_names['#s'] = 'status'
        
    if not update_expr_parts: return

    update_expr = "SET " + ", ".join(update_expr_parts)
    kwargs = {
        'Key': {'batch_id': batch_id},
        'UpdateExpression': update_expr,
        'ExpressionAttributeValues': expr_vals
    }
    if expr_names: kwargs['ExpressionAttributeNames'] = expr_names

    try:
        tracker_table.update_item(**kwargs)
        print(f"DynamoDB updated to {batch_id}")
    except Exception as e:
        print(f"Error updating DynamoDB: {e}")