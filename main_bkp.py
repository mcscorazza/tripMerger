import os

import time
from dotenv import load_dotenv
import boto3
import pandas as pd
import awswrangler as wr

from boto3.dynamodb.conditions import Key
from utils import *



# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================

load_dotenv()


TRACKER_TABLE_NAME = os.environ.get('TRACKER_TABLE_NAME')
GSI_NAME = os.environ.get('GSI_NAME')
DB_HOST = os.environ.get('DB_HOST', 'trips-sumary-db.cvwwgqyg0i2a.sa-east-1.rds.amazonaws.com')
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '.$MCSystem26$.')

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
tracker_table = dynamodb.Table(TRACKER_TABLE_NAME)





# ==========================================
# 3. ATUALIZAÇÃO NO DYNAMODB (trip_state_tracker)
# ==========================================




# ==========================================
# 5. O CORAÇÃO DA LAMBDA: MÁQUINA DE FATIAR (LOOP)
# ==========================================
def process_trip_data(batch_id):
    raw_path = f"s3://{BUCKET_NAME}/raw/batch_id={batch_id}/"
    
    try:
        raw_files = wr.s3.list_objects(path=raw_path)
    except Exception as e:
        print(f"Aviso: Não foi possível listar arquivos para {batch_id}. Detalhes: {e}")
        return False
        
    if not raw_files:
        return False
        
    # Ordena alfabeticamente para garantir a linha do tempo (seq_001, seq_002...)
    raw_files = sorted(raw_files)
    print(f"--> Iniciando processamento para {batch_id}. Total na fila: {len(raw_files)} arquivos.")

    # ----------------------------------------------------
    # LOOP PRINCIPAL: Roda enquanto houver arquivos na fila
    # ----------------------------------------------------
    while len(raw_files) > 0:
        
        # Fatia estritamente 600 arquivos por vez
        if len(raw_files) >= 600:
            files_to_process = raw_files[:600]
        else:
            files_to_process = raw_files
            
        print(f"\n🔄 Extraindo lote de {len(files_to_process)} arquivos...")

        # Lê apenas a fatia atual (lembrando de usar lines=True para o JSON)
        df = wr.s3.read_json(path=files_to_process, orient='records', lines=True)
        if 'batch_seq' in df.columns:
            df = df.sort_values('batch_seq')

        # --- EXTRAÇÃO DO TIMESTAMP DO HARDWARE ---
        if 'battery' in df.columns:
            df['device_ts'] = df['battery'].apply(lambda x: x.get('timestamp') if isinstance(x, dict) else None)
        else:
            df['device_ts'] = None

        is_first_batch = (df['batch_seq'].min() == 1) if 'batch_seq' in df.columns else False
        is_finished = ('FINISH' in df['trip_status'].values) if 'trip_status' in df.columns else False

        # Extrai as Coordenadas para o Mapa e Cidades
        if 'position' in df.columns:
            df['lat'] = df['position'].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) >= 2 else None)
            df['lng'] = df['position'].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) >= 2 else None)
            geo_df = df[['device_ts', 'lat', 'lng']].copy().dropna(subset=['lat', 'lng'])
        else:
            geo_df = pd.DataFrame()

        first_valid_position = None
        current_valid_position = None
        
        if not geo_df.empty:
            if is_first_batch:
                first_valid_position = [float(geo_df['lat'].iloc[0]), float(geo_df['lng'].iloc[0])]
            current_valid_position = [float(geo_df['lat'].iloc[-1]), float(geo_df['lng'].iloc[-1])]

        # ----------------------------------------------------
        # A TRAVA INTELIGENTE (Para o resto da fila)
        # ----------------------------------------------------
        quantidade_pacotes = len(df)
        
        if quantidade_pacotes < 600 and not is_finished:
            print(f"⏳ Restaram {quantidade_pacotes} pacotes na fila. Aguardando o hardware enviar o resto...")
            
            # O early-update: Salva a cidade no BD se o pacote 1 estiver nesse resto
            if is_first_batch and first_valid_position:
                print("📍 Pacote #1 detectado no resto da fila! Atualizando cidade de origem no DynamoDB...")
                update_trip_state(
                    batch_id=batch_id, 
                    position_start=first_valid_position, 
                    position_current=current_valid_position, 
                    is_finished=False
                )
                
            break # QUEBRA O LOOP WHILE. Deixa o resto no S3 e encerra a Lambda.

        # ----------------------------------------------------
        # CONSOLIDAÇÃO (Se tem 600 cravados ou se for o FINISH)
        # ----------------------------------------------------
        ts_inicial = int(df['device_ts'].min()) if not df['device_ts'].isnull().all() else int(time.time() - 600)
        ts_final = int(df['device_ts'].max()) if not df['device_ts'].isnull().all() else int(time.time())
        
        short_start = to_base62(ts_inicial)
        short_end = to_base62(ts_final)
        parquet_filename = f"{short_start}_{short_end}.parquet"
        
        if not geo_df.empty:
            geo_df.rename(columns={'device_ts': 't'}, inplace=True)
            geo_points = geo_df.to_dict(orient='records') 
            if geo_points:
                save_trip_segment_to_rds(batch_id, ts_inicial, ts_final, geo_points, parquet_filename)

        update_trip_state(
            batch_id=batch_id,
            position_start=first_valid_position,
            position_current=current_valid_position,
            is_finished=is_finished
        )

        s3_parquet_key = f"s3://{BUCKET_NAME}/consolidated/batch_id={batch_id}/{parquet_filename}"
        wr.s3.to_parquet(df=df, path=s3_parquet_key, index=False)
        print(f"✅ Parquet gerado com {quantidade_pacotes} linhas: {parquet_filename}")

        # Deleta APENAS os arquivos que foram lidos neste loop
        wr.s3.delete_objects(path=files_to_process)
        print(f"🗑️ Limpeza: {len(files_to_process)} JSONs deletados do S3.")
        
        # ----------------------------------------------------
        # ATUALIZA A FILA PARA A PRÓXIMA VOLTA DO LOOP
        # ----------------------------------------------------
        raw_files = raw_files[len(files_to_process):]
        
    return True

# ==========================================
# 6. HANDLER PRINCIPAL (Gatilho da Lambda)
# ==========================================
print("--- Waking up Consolidation Worker ---")
try:
    response = tracker_table.query(
        IndexName=GSI_NAME,
        KeyConditionExpression=Key('status').eq('CONSOLIDATED')
    )
    pending_trips = response.get('Items', [])
except Exception as e:
    print(f"Error querying DynamoDB GSI: {e}")
    
if not pending_trips:
    print("No pending trips to process. Going back to sleep.")
    
for trip in pending_trips:
    print(trip['batch_id'])
    #batch_id = trip['batch_id']
    #print(f"\n--- Starting consolidation for batch: {batch_id} ---")
    # process_trip_data(batch_id)