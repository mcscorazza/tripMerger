import os
import awswrangler as wr
import pandas as pd
import numpy as np
from database import get_db_connection, release_db_connection
from dynamo import tracker_table
from boto3.dynamodb.conditions import Key
from rainflow.main import CalculadoraDanoFadiga

BUCKET_NAME = os.environ.get('BUCKET_NAME')
CALC_GSI_NAME = os.environ.get('CALC_GSI_NAME')

calc_fadiga = CalculadoraDanoFadiga(limite_fat=60.85)

def get_pending_calculations():
    print("🔍 Buscando viagens pendentes de cálculo no DynamoDB...")
    try:
        response = tracker_table.query(
            IndexName=CALC_GSI_NAME,
            KeyConditionExpression=Key('calc_status').eq('PENDING')
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"❌ Erro ao buscar no DynamoDB: {e}")
        return []

def get_uncalculated_chunks(batch_id):
    conn = get_db_connection()
    if not conn: return []
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT id, parquet_ref 
            FROM trip_geolocations 
            WHERE batch_id = %s AND damage IS NULL
        """
        cursor.execute(query, (batch_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"❌ Erro ao buscar chunks no RDS: {e}")
        return []
    finally:
        if 'cursor' in locals(): cursor.close()
        release_db_connection(conn)

def calculate_rainflow(batch_id, parquet_ref):
    s3_path = f"s3://{BUCKET_NAME}/consolidated/batch_id={batch_id}/{parquet_ref}.parquet"
    
    try:
        df_bruto_parquet = wr.s3.read_parquet(path=s3_path)
        df_exp_sensors = df_bruto_parquet.explode('sensors')
        df_norm_sensors = pd.json_normalize(df_exp_sensors['sensors'])
        df_values = df_norm_sensors.explode('value')

        array_sg15 = df_values['value'].astype(float).to_numpy()
        print(array_sg15)
        damage_value = calc_fadiga.calcular_dano(array_sg15)
        
        return damage_value
    except Exception as e:
        print(f"❌ Erro ao ler Parquet ou calcular fadiga ({parquet_ref}): {e}")
        return None

def update_chunk_damage(chunk_id, damage_value):
    conn = get_db_connection()
    if not conn: return
    
    try:
        cursor = conn.cursor()
        query = "UPDATE trip_geolocations SET damage = %s WHERE id = %s"
        cursor.execute(query, (damage_value, chunk_id))
        conn.commit()
    except Exception as e:
        print(f"❌ Erro ao atualizar damage no RDS: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        release_db_connection(conn)

def update_dynamo_calc_status(batch_id):
    try:
        tracker_table.update_item(
            Key={'batch_id': batch_id},
            UpdateExpression="SET calc_status = :status",
            ExpressionAttributeValues={':status': 'CALCULATED'}
        )
        print(f"✅ Viagem {batch_id} marcada como CALCULATED no DynamoDB.")
    except Exception as e:
        print(f"❌ Erro ao atualizar DynamoDB: {e}")

def run_calculator():
    print("🚀 Iniciando Motor de Cálculo de Danos (Rainflow)...")
    pending_trips = get_pending_calculations()
    
    for trip in pending_trips:
        batch_id = trip['batch_id']
        trip_status = trip.get('status') # PENDING ou CONSOLIDATED
        
        print(f"\n⚙️ Processando viagem: {batch_id} (Status: {trip_status})")
        
        uncalculated_chunks = get_uncalculated_chunks(batch_id)
        
        if not uncalculated_chunks:
            print("   👍 Todos os chunks já foram calculados para esta viagem.")
        else:
            for chunk_id, parquet_ref in uncalculated_chunks:
                print(f"   📊 Calculando Rainflow para o chunk: {parquet_ref}")
                damage_val = calculate_rainflow(batch_id, parquet_ref)
                
                if damage_val is not None:
                    update_chunk_damage(chunk_id, damage_val)
                    print(f"   ✔️ Dano salvo ({damage_val:.10f}) no RDS.")

        if trip_status == 'CONSOLIDATED':
            chunks_left = get_uncalculated_chunks(batch_id)
            if not chunks_left:
                update_dynamo_calc_status(batch_id)

if __name__ == "__main__":
    run_calculator()