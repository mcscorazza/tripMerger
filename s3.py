import os
import awswrangler as wr

BUCKET_NAME = os.environ.get('BUCKET_NAME')


# ----------------------------------------------------------------------------
#   GET S3 JSON FILES BY BATCH ID
# ----------------------------------------------------------------------------
def get_s3_objects(batch_id):
  raw_path = f"s3://{BUCKET_NAME}/raw/batch_id={batch_id}/"

  try:
    raw_files = wr.s3.list_objects(path=raw_path)

  except Exception as e:
    print(f"Warning: Could not list files for {batch_id}. Details: {e}")
    return []

  if not raw_files:
    return []

  return raw_files    

# ----------------------------------------------------------------------------
#   LIST PENDING TRIPS S3 JSON FILES QUANTITY
# ----------------------------------------------------------------------------
def print_s3_list(pending_trips):

  print("\n# Reading S3 bucket:")
  print("  +-------------------------------------------+-------------------+")
  print("  | BATCH ID                                  |        JSON FILES |")
  print("  |-------------------------------------------|-------------------|")

  for trip in pending_trips:
    raw_files = get_s3_objects(trip['batch_id'])
    print(f"  | {trip['batch_id']}      | {len(raw_files):6} json files |")
    print("  +-------------------------------------------+-------------------+")


def has_finish(json_file):
  df = wr.s3.read_json(path=json_file, orient='records', lines=True)
  finish = ('FINISH' in df['trip_status'].values) if 'trip_status' in df.columns else False
  return finish

def has_start(json_file):
  df = wr.s3.read_json(path=json_file, orient='records', lines=True)
  first = (df['trip_status'].item() == "START") if 'batch_seq' in df.columns else False
  return first