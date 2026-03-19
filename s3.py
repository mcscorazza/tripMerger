import os
import math
import json
from database import *
from utils import *
import awswrangler as wr
import pandas as pd
import time

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

def chunk_type(json_files, chunk_size):
  is_start = has_start(json_files[0])
  is_finish = has_finish(json_files[-1])

  print("    Has START: ", is_start)
  print("    Has FINISH: ", is_finish)
  print("    JSON Qty: ", len(json_files))
 
  if (len(json_files) < chunk_size) and (not is_start) and (not is_finish):
    return 0
  if (len(json_files) < chunk_size) and (is_start) and (not is_finish):
    return 1    
  if (len(json_files) < chunk_size) and (is_start) and (is_finish):
    return 2
  if (len(json_files) < chunk_size) and (not is_start) and (is_finish):
    return 3
  if len(json_files) > chunk_size  and (not is_finish):
    return 4
  if len(json_files) > chunk_size  and (is_finish):
    return 5
  return -1


def process_json_files(id, json_files):
  df = wr.s3.read_json(path=json_files, orient='records', lines=True)
  if 'batch_seq' in df.columns:
    df = df.sort_values('batch_seq')

  if 'battery' in df.columns:
    df['ts'] = df['battery'].apply(lambda x: x.get('timestamp') if isinstance(x, dict) else None)
  else:
    df['ts'] = None

  ts_start = int(df['ts'].min()) if not df['ts'].isnull().all() else int(time.time() - 600)
  ts_finish = int(df['ts'].max()) if not df['ts'].isnull().all() else int(time.time())

  print("     ⏱ TS Start: ", ts_start)
  print("     ⏱ TS Finish: ", ts_finish)

  short_start = to_base62(ts_start)
  short_end = to_base62(ts_finish)
  parquet_filename = f"{short_start}_{short_end}"

  chart_data = []
  chunk_sum = 0.0
  chunk_count = 0

  if 'sensors' in df.columns:
    for index, row in df.iterrows():
      sensors = row.get('sensors')
      row_ts = row.get('ts')
      
      if isinstance(sensors, list):
        for sensor in sensors:
          if isinstance(sensor, dict) and sensor.get('id') == 'Truque_A':
            valores_brutos = sensor.get('value')
            
            if isinstance(valores_brutos, list):
              valores_limpos = []
              for v in valores_brutos:
                try:
                  num = float(v)
                  if not math.isnan(num):
                    valores_limpos.append(num)
                except (ValueError, TypeError):
                  pass
              
              if valores_limpos:
                max_val = max(valores_limpos)
                min_val = min(valores_limpos)
                avg_val = sum(valores_limpos) / len(valores_limpos)
                
                chart_data.append({
                  't': int(row_ts) if pd.notnull(row_ts) else 0,
                  'max': round(max_val, 2),
                  'min': round(min_val, 2),
                  'avg': round(avg_val, 2)
                })
                
                chunk_sum += avg_val
                chunk_count += 1
          
            break

  if 'position' in df.columns:
      df['lat'] = df['position'].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) >= 2 else None)
      df['lng'] = df['position'].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) >= 2 else None)
      geo_df = df[['ts', 'lat', 'lng']].copy().dropna(subset=['lat', 'lng'])
  else:
      geo_df = pd.DataFrame()

  if not geo_df.empty:
    geo_df.rename(columns={'ts': 't'}, inplace=True)
    geo_points = geo_df.to_dict(orient='records') 
    if geo_points:
      save_chunk_to_rds(id, ts_start, ts_finish, geo_points, parquet_filename, 
              json.dumps(chart_data), chunk_sum, chunk_count)
  
  s3_parquet_key = f"s3://{BUCKET_NAME}/consolidated/batch_id={id}/{parquet_filename}.parquet"
  wr.s3.to_parquet(df=df, path=s3_parquet_key, index=False)
  print(f"     ✅ Parquet generated with {len(df)} lines: {parquet_filename}")

  wr.s3.delete_objects(path=json_files)
  print(f"     🗑️ Cleaning: {len(json_files)} JSONs removed from S3.")

  return True

def large_process_data(id, json_files, chunk_size, is_finish = False):
  loop_num = 1
  while len(json_files) > 0:
    print("\n    ---- Loop #", loop_num)
    print("\n     - ⚙ Total JSON files: ", len(json_files))
    
    if len(json_files) >= chunk_size:
      files_to_process = json_files[:chunk_size]
    else:
      files_to_process = json_files
    
    print("     - ⚙ JSON files to process: ", len(files_to_process))
    
    print(f"\n     🔄 Extracting batch from {len(files_to_process)} JSON files...")

    process_chunk = process_json_files(id, files_to_process)
    
    if process_chunk:
      print("     ✅ JSON files processed!")
      json_files = json_files[len(files_to_process):]

    if len(json_files) < chunk_size and (not is_finish):
      print("\n    ---- END!")
      print("     ⏹ No FINISH found! Exit #JSON files: ", len(json_files))
      break

    loop_num += 1

def get_pos(json):
  df = wr.s3.read_json(path=json, orient='records', lines=True)
  if 'position' in df.columns:
    df['lat'] = df['position'].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) >= 2 else None)
    df['lng'] = df['position'].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) >= 2 else None)
    return [float(df['lat'].iloc[0]), float(df['lng'].iloc[0])]