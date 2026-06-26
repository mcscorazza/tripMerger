import os
import math
import json
import time
import pandas as pd
import awswrangler as wr
from database import save_chunk_to_rds
from utils import to_base62

BUCKET_NAME = os.environ.get('BUCKET_NAME')

def get_s3_objects(batch_id):
  raw_path = f"s3://{BUCKET_NAME}/raw/batch_id={batch_id}/"
  try:
    raw_files = wr.s3.list_objects(path=raw_path)
    return raw_files if raw_files else []
  except Exception as e:
    print(f"Aviso: Não foi possível listar ficheiros para {batch_id}. Detalhes: {e}")
    return []

def print_s3_list(pending_trips):
  print("\n# A ler o bucket S3:")
  print("  +-------------------------------------------+-------------------+")
  print("  | BATCH ID                                  |        JSON FILES |")
  print("  |-------------------------------------------|-------------------|")
  for trip in pending_trips:
    raw_files = get_s3_objects(trip['batch_id'])
    print(f"  | {trip['batch_id']}      | {len(raw_files):6} json files |")
    print("  +-------------------------------------------+-------------------+")

def inspect_file_edge(json_file):
  """Lê o ficheiro uma única vez para extrair os metadados necessários das pontas."""
  try:
    df = wr.s3.read_json(path=json_file, orient='records', lines=True)
    if df.empty:
      return False, False, None
    
    is_finish = ('FINISH' in df['trip_status'].values) if 'trip_status' in df.columns else False
    is_start = False

    if 'batch_seq' in df.columns and 'trip_status' in df.columns:
      is_start = (df['trip_status'].iloc[0] == "START")
    
    pos = None

    if 'position' in df.columns and len(df) > 0:
      first_row_pos = df['position'].iloc[0]
      if isinstance(first_row_pos, list) and len(first_row_pos) >= 2:
        pos = [float(first_row_pos[0]), float(first_row_pos[1])]
    return is_start, is_finish, pos
  except Exception as e:
    print(f"❌ Erro ao inspecionar o ficheiro {json_file}: {e}")
    return False, False, None

def chunk_type_optimized(json_files, chunk_size):
  if not json_files:
    return -1
  is_start, _, _ = inspect_file_edge(json_files[0])
  _, is_finish, _ = inspect_file_edge(json_files[-1])

  print("    Has START: ", is_start)
  print("    Has FINISH: ", is_finish)
 
  total_files = len(json_files)
  if total_files < chunk_size:
    if not is_start and not is_finish: return 0
    if is_start and not is_finish: return 1    
    if is_start and is_finish: return 2
    if not is_start and is_finish: return 3
  else:
    if not is_finish: return 4
    if is_finish: return 5
  return -1

def process_json_files(batch_id, files_to_process):
  if not files_to_process:
    return False
  
  df = wr.s3.read_json(path=files_to_process, orient='records', lines=True)
  if df.empty:
    return False

  if 'batch_seq' in df.columns:
    df = df.sort_values('batch_seq')

  if 'battery' in df.columns:
    df['ts'] = df['battery'].apply(lambda x: x.get('timestamp') if isinstance(x, dict) else None)
  else:
    df['ts'] = None

  ts_start = int(df['ts'].min()) if not df['ts'].isnull().all() else int(time.time() - 600)
  ts_finish = int(df['ts'].max()) if not df['ts'].isnull().all() else int(time.time())

  print("     ⏱  TS Start: ", ts_start)
  print("     ⏱  TS Finish: ", ts_finish)

  short_start = to_base62(ts_start)
  short_end = to_base62(ts_finish)
  parquet_filename = f"{short_start}_{short_end}"

  chart_data = []
  chunk_sum = 0.0
  chunk_count = 0

  if 'sensors' in df.columns:
    for _, row in df.iterrows():
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
      save_chunk_to_rds(batch_id, ts_start, ts_finish, geo_points, parquet_filename, 
                              json.dumps(chart_data), chunk_sum, chunk_count)
    
  s3_parquet_key = f"s3://{BUCKET_NAME}/consolidated/batch_id={batch_id}/{parquet_filename}.parquet"
  wr.s3.to_parquet(df=df, path=s3_parquet_key, index=False)
  print(f"     ✅ Parquet gerado com {len(df)} linhas: {parquet_filename}")

  wr.s3.delete_objects(path=files_to_process)
  print(f"     🗑️ Limpeza: {len(files_to_process)} JSONs removidos do S3.")
  return True

def large_process_data(batch_id, json_files, chunk_size, is_finish=False):
  loop_num = 1
  while len(json_files) > 0:
    print(f"\n    ---- Loop #{loop_num}")
    print("     - ⚙ Total JSON files em fila: ", len(json_files))
    
    files_to_process = json_files[:chunk_size]
    print("     - ⚙ JSON files a processar neste loop: ", len(files_to_process))
    
    success = process_json_files(batch_id, files_to_process)
    
    if success:
      json_files = json_files[len(files_to_process):]
    else:
      print("     ❌ Falha crítica no processamento do lote. Interrompendo loop para evitar perda de dados.")
      break

    if len(json_files) < chunk_size and not is_finish:
      print("    ---- END CHUNK!")
      print("     ⏹ Sem sinal de FINISH. Ficheiros restantes em cache: ", len(json_files))
      break

    loop_num += 1