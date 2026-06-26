import os
import json
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_NAME = os.environ.get('DB_NAME')

# Inicializa o pool de conexões (Mínimo: 1, Máximo: 10 conexões ativas)
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )
    print("✅ Pool de conexões do RDS inicializado com sucesso.")
except Exception as e:
    print(f"❌ Erro ao inicializar o Pool do RDS: {e}")
    db_pool = None

def get_db_connection():
    if db_pool:
        return db_pool.getconn()
    return None

def release_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

def select_rds_trips(limit=5):
    conn = get_db_connection()
    if not conn:
        return []
    
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            SELECT batch_id, start_timestamp, end_timestamp, parquet_ref 
            FROM trip_geolocations 
            ORDER BY start_timestamp DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        return cursor.fetchall()
    except Exception as e:
        print(f"❌ Erro ao consultar a tabela: {e}")
        return []
    finally:
        if cursor: 
            cursor.close()
        release_db_connection(conn)

def list_rds_trips(limit):
    rds_trips = select_rds_trips(limit=limit)

    print("+----------------------------------------+------------+------------+--------------------------+")
    print("| Batch ID                               | ts Start   | ts End     | Parquet Ref.             |")
    print("+----------------------------------------+------------+------------+--------------------------+")
    
    for rds_trip in rds_trips:
        print(f"| {rds_trip[0]:38} | {rds_trip[1]:10} | {rds_trip[2]:10} | {rds_trip[3]:24} |")

    print("+----------------------------------------+------------+------------+--------------------------+\n")

def save_chunk_to_rds(batch_id, ts_start, ts_end, geo_points, parquet_ref, chart_data_json, chunk_sum, chunk_count):
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = None
    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO trip_geolocations 
            (batch_id, start_timestamp, end_timestamp, geo_points, parquet_ref, chart_data, chunk_sum, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            batch_id, 
            ts_start, 
            ts_end, 
            json.dumps(geo_points), 
            parquet_ref, 
            chart_data_json,
            chunk_sum, 
            chunk_count
        ))
        conn.commit()
        print(f"     📋 Batch salvo no RDS com ref: {parquet_ref}")
    except Exception as e:
        if conn: 
            conn.rollback()
        print(f"❌ Erro no RDS: {e}")
    finally:
        if cursor: 
            cursor.close()
        release_db_connection(conn)