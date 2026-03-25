import sys
from post_process import trip_critical_analisis

if len(sys.argv) > 1:
    batch_id = sys.argv[1]
    trip_critical_analisis(batch_id=batch_id)
    print(f"Iniciando o pós-processamento para o batch_id: {batch_id}")
else:
    print("Erro: Você precisa fornecer um batch_id.")
    print("Uso correto: python3 ./post_process.py <batch_id>")