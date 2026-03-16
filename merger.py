from database import *
from s3 import *
from dynamo import *


def process_data(batch_id, json_files, process_type, chunk_size):

  match process_type:
    case 0:
      print("    👍🏻 Nothing to do!")
    
    case 1:
      print("    🚩 Just update start point on trips table.")
      start_pos = get_pos(json_files[0])
      update_trip_state(batch_id=batch_id, start_pos=start_pos)
    
    case 2:
      print("    ✅ Complete short trip. Process data and update Start and Finish")
      start_pos = get_pos(json_files[0])
      finish_pos = get_pos(json_files[-1])
      process_json_files(batch_id, json_files)
      update_trip_state(batch_id=batch_id, start_pos=start_pos, current_pos=finish_pos, is_finished=True)

    case 3:
      print("    ✅ Last chunk. Process data and update Finish")
      finish_pos = get_pos(json_files[-1])
      process_json_files(batch_id, json_files)
      update_trip_state(batch_id=batch_id, current_pos=finish_pos, is_finished=True)
  
    case 4:
      print("    ⏩ Complete large trip without FINISH. Process chunks.")
      finish_pos = get_pos(json_files[-1])
      large_process_data(batch_id, json_files, chunk_size, is_finish=False)
      update_trip_state(batch_id=batch_id, current_pos=finish_pos, is_finished=False)

    case 5:
      print("    ✅ Complete large trip with FINISH. Process chunks.")
      finish_pos = get_pos(json_files[-1])
      large_process_data(batch_id, json_files, chunk_size, is_finish=True)
      update_trip_state(batch_id=batch_id, current_pos=finish_pos, is_finished=True)
    case _:
      print("    ❌ Error!")


