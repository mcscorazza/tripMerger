from database import *
from s3 import *
from dynamo import *
import pandas as pd

def complete_process_data(files):
  print("Complete process data")
    
def update_start(id, json):
  df = wr.s3.read_json(path=json, orient='records', lines=True)
  if 'position' in df.columns:
    df['lat'] = df['position'].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) >= 2 else None)
    df['lng'] = df['position'].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) >= 2 else None)
    start_pos = [float(df['lat'].iloc[0]), float(df['lng'].iloc[0])]
    update_trip_state(id, start_pos=start_pos)