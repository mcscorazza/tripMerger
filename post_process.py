from database import get_db_connection, release_db_connection
from psycopg2.extras import execute_values

def trip_critical_analisis(batch_id):
    print(f"🔍 Starting Post-Trip Criticality Analysis: {batch_id}")
    
    try:
        conn = get_db_connection()
        if not conn: return
        cursor = conn.cursor()

        query_avg = """
            SELECT SUM(chunk_sum) / NULLIF(SUM(chunk_count), 0) AS global_avg
            FROM trip_geolocations
            WHERE batch_id = %s
        """
        cursor.execute(query_avg, (batch_id,))
        result = cursor.fetchone()
        
        if not result or result[0] is None:
            print("     ❌ Insufficient data to calculate the global average.")
            return

        global_avg = abs(float(result[0]))
        critical_limit = global_avg * 1.5
        
        print(f"     📊 Global Average: {global_avg:.2f} kgf | 🚨 Critical Trigger: > {critical_limit:.2f} kgf")

        query_chunks = """
            SELECT id, chart_data 
            FROM trip_geolocations 
            WHERE batch_id = %s
        """
        cursor.execute(query_chunks, (batch_id,))
        chunks = cursor.fetchall()
        
        critical_ids = []

        for chunk in chunks:
            chunk_id = chunk[0]
            chart_data = chunk[1]
            
            if not chart_data: continue

            max_peak = max([abs(point.get('max', 0)) for point in chart_data])
            compress_peak = max([abs(point.get('min', 0)) for point in chart_data])
            
            abs_peak = max(max_peak, compress_peak)

            if (abs_peak > critical_limit):
                critical_ids.append((chunk_id,))

        if critical_ids:
            update_query = """
                UPDATE trip_geolocations AS t
                SET is_critical = TRUE
                FROM (VALUES %s) AS c(id)
                WHERE t.id = c.id
            """
            execute_values(cursor, update_query, critical_ids)
            conn.commit()
            print(f"     ✅ Analysis complete! {len(critical_ids)} sections marked as CRITICAL.")
        else:
            print("     ✅ Analysis complete! No critical sections found (Smooth trip).")

        cursor.close()
        release_db_connection(conn)

    except Exception as e:
        print(f"     ❌ Error in the Parsing Engine: {e}")