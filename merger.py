from s3 import inspect_file_edge, process_json_files, large_process_data
from dynamo import update_trip_state
from post_process import trip_critical_analisis

def process_data(batch_id, json_files, process_type, chunk_size):
  if not json_files:
      return

  is_start, _, first_pos = inspect_file_edge(json_files[0])
  _, _, finish_pos = inspect_file_edge(json_files[-1])

  start_pos = first_pos if is_start else None

  match process_type:
    case 0:
      print("\n    👍🏻 Nada a fazer!")
    
    case 1:
      print("\n    🚩 Apenas atualizar ponto de partida na tabela DynamoDB.")
      update_trip_state(batch_id=batch_id, start_pos=start_pos)
    
    case 2:
      print("\n    ✅ Viagem curta completa. Processar e atualizar Partida e Fim.")
      process_json_files(batch_id, json_files)
      trip_critical_analisis(batch_id)
      update_trip_state(batch_id=batch_id, start_pos=start_pos, current_pos=finish_pos, is_finished=True)

    case 3:
      print("\n    ✅ Último chunk. Processar dados e atualizar Fim.")
      process_json_files(batch_id, json_files)
      trip_critical_analisis(batch_id)
      update_trip_state(batch_id=batch_id, current_pos=finish_pos, is_finished=True)
  
    case 4:
      print("\n    ⏩ Viagem longa sem FINISH. Processar por chunks recorrentes.")
      large_process_data(batch_id, json_files, chunk_size, is_finish=False)
      update_trip_state(batch_id=batch_id, start_pos=start_pos, current_pos=finish_pos, is_finished=False)
      trip_critical_analisis(batch_id)

    case 5:
      print("\n    ✅ Viagem longa com FINISH. Processar chunks e fechar.")
      large_process_data(batch_id, json_files, chunk_size, is_finish=True)
      update_trip_state(batch_id=batch_id, start_pos=start_pos, current_pos=finish_pos, is_finished=True)
      trip_critical_analisis(batch_id)

    case _:
      print("\n    ❌ Tipo de processo desconhecido!")