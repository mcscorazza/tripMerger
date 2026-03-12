import os
import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

load_dotenv()

TRACKER_TABLE_NAME = os.environ.get('TRACKER_TABLE_NAME')
GSI_NAME = os.environ.get('GSI_NAME')
REGION = os.environ.get('AWS_DEFAULT_REGION') 

try:
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    tracker_table = dynamodb.Table(TRACKER_TABLE_NAME)
except Exception as e:
    print(f"Erro ao inicializar boto3 para DynamoDB: {e}")


def buscar_viagens_pendentes():
    """Consulta o DynamoDB (via GSI) para encontrar viagens com status PENDING."""
    try:
        response = tracker_table.query(
            IndexName=GSI_NAME,
            KeyConditionExpression=Key('status').eq('PENDING')
        )
        viagens = response.get('Items', [])
        return viagens
    except Exception as e:
        print(f"❌ Erro ao buscar no DynamoDB: {e}")
        return []