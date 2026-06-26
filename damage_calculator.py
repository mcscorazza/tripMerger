import os
import awswrangler as wr
from database import get_db_connection, release_db_connection
from dynamo import tracker_table  # Importando a tabela do seu dynamo.py
from boto3.dynamodb.conditions import Key

BUCKET_NAME = os.environ.get('BUCKET_NAME')
CALC_GSI_NAME = os.environ.get('CALC_GSI_NAME')

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
        df = wr.s3.read_parquet(path=s3_path)
        damage_value = 12.5
        
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
                    print(f"   ✔️ Dano salvo ({damage_val:.2f}) no RDS.")

        # Regra de Ouro: Só marcamos como CALCULATED se a ingestão já finalizou (CONSOLIDATED)
        # e não há mais chunks sem cálculo no banco.
        if trip_status == 'CONSOLIDATED':
            chunks_left = get_uncalculated_chunks(batch_id)
            if not chunks_left:
                update_dynamo_calc_status(batch_id)

if __name__ == "__main__":
    run_calculator()