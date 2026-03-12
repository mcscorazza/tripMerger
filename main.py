from database import listar_geolocalizacoes
from dynamo import buscar_viagens_pendentes

def iniciar_worker():
    print("Iniciando os testes modulares do Worker...\n")
    
    # ---------------------------------------------------------
    # PASSO 1: Banco de Dados Relacional (RDS)
    # ---------------------------------------------------------
    print("--- Testando Conexão RDS ---")
    listar_geolocalizacoes(limite=3) # Diminuí o limite só para poluir menos a tela
    
    # ---------------------------------------------------------
    # PASSO 2: Banco de Dados NoSQL (DynamoDB)
    # ---------------------------------------------------------
    print("\n--- Testando Leitura do DynamoDB ---")
    viagens_pendentes = buscar_viagens_pendentes()
    
    if not viagens_pendentes:
        print("✅ Nenhuma viagem pendente encontrada no momento.")
    else:
        print(f"⚠️   Encontradas {len(viagens_pendentes)} viagens com status PENDING:")
        for viagem in viagens_pendentes:
            batch_id = viagem.get('batch_id')
            ts_inicio = viagem.get('started_at', 'Desconhecido')
            print(f" -> Batch ID: {batch_id} | Criado em: {ts_inicio}")

if __name__ == "__main__":
    iniciar_worker()