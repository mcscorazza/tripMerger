import os
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_NAME = os.environ.get('DB_NAME')

def conectar_rds():
    """Cria e retorna uma conexão com o banco de dados RDS."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar no RDS: {e}")
        return None

def listar_geolocalizacoes(limite=5):
    """Busca os últimos registros salvos na tabela trip_geolocations."""
    conn = conectar_rds()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        # Uma query simples para listar os dados sem trazer o JSON pesado do mapa
        query = """
            SELECT batch_id, start_timestamp, end_timestamp, parquet_ref 
            FROM trip_geolocations 
            ORDER BY start_timestamp DESC 
            LIMIT %s
        """
        cursor.execute(query, (limite,))
        linhas = cursor.fetchall()
        
        print(f"\n--- Últimos {limite} registros no RDS ---")
        if not linhas:
            print("A tabela está vazia.")
        else:
            for linha in linhas:
                batch = linha[0][:8] # Pegando só os primeiros 8 caracteres do ID
                ts_inicio = linha[1]
                ts_fim = linha[2]
                parquet = linha[3]
                print(f"Viagem: {batch}... | Início: {ts_inicio} | Fim: {ts_fim} | Parquet: {parquet}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao consultar a tabela: {e}")