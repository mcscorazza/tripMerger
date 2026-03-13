from database import *
from s3 import *
from dynamo import *
from merger import *

CHUNK_SIZE = 600

def init_merger():
  print("-- Starting the Merger --\n")

  # Search pending trips on DynamoDB table
  pending_trips = search_pending_trips()

  if pending_trips:
    # List the qty of json files (if pending trips found!)
    print_s3_list(pending_trips)

  for trip in pending_trips:
    print(f"\nGet infos for trip: {trip['batch_id']}")
    jsons_files = get_s3_objects(trip['batch_id'])
    print(f" > Found: {len(jsons_files)} json files")
    ordered_jsons = sorted(jsons_files)
    is_start = has_start(ordered_jsons[0])
    is_finish = has_finish(ordered_jsons[-1])    
    
    if (len(ordered_jsons) < CHUNK_SIZE) and (not is_start) and (not is_finish):
      print("Nothing to do!")
    
    if (len(ordered_jsons) < CHUNK_SIZE) and (is_start) and (not is_finish):
      update_start(trip['batch_id'], ordered_jsons[0])
      
    if len(ordered_jsons) < CHUNK_SIZE & is_start & is_finish:
      complete_process_data(ordered_jsons)
    
    


  print("\n-- Merger Finished --\n")

if __name__ == "__main__":
    init_merger()