import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_NAME = os.environ.get('DB_NAME')

def conect_rds():
    try:
        conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
        return conn
    except Exception as e:
        print(f"❌ Error on conect to RDS: {e}")
        return None

def select_rds_trips(limit=5):
    conn = conect_rds()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        query = """
            SELECT batch_id, start_timestamp, end_timestamp, parquet_ref 
            FROM trip_geolocations 
            ORDER BY start_timestamp DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        lines = cursor.fetchall()
        return lines
    
    except Exception as e:
        print(f"❌ Error to query table: {e}")
        return []
    
    finally:
        cursor.close()
        conn.close()

def list_rds_trips(limit):
    rds_trips = select_rds_trips(limit=limit)

    print("+----------------------------------------+------------+------------+--------------------------+")
    print("| Batch ID                               | ts Start   | ts End     | Parquet Ref.             |")
    print("+----------------------------------------+------------+------------+--------------------------+")
    
    for rds_trip in rds_trips:
        print(f"| {rds_trip[0]:38} | {rds_trip[1]:10} | {rds_trip[2]:10} | {rds_trip[3]:24} |")

    print("+----------------------------------------+------------+------------+--------------------------+\n")

# =========================================
#  Recording coordinates in RDS PostgreSQL
# =========================================
def save_chunk_to_rds(batch_id, ts_start, ts_end, geo_points, parquet_ref):
    try:
        conn = conect_rds()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        query = """
            INSERT INTO trip_geolocations (batch_id, start_timestamp, end_timestamp, geo_points, parquet_ref)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (batch_id, ts_start, ts_end, json.dumps(geo_points), parquet_ref))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"     📋 Batch saved in RDS with ref: {parquet_ref}")
    except Exception as e:
        print(f"Error on RDS: {e}")