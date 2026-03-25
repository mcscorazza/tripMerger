from database import *
from s3 import *
from dynamo import *
from merger import *

CHUNK_SIZE = 600

def init_merger():
  print("-- Starting the Merger --\n")

  pending_trips = search_pending_trips()

  if pending_trips:
    print_s3_list(pending_trips)

  for trip in pending_trips:
    print(f"\nGet infos for trip: {trip['batch_id']}")
    jsons_files = get_s3_objects(trip['batch_id'])
    ordered_jsons = sorted(jsons_files)
    process_type = chunk_type(ordered_jsons, CHUNK_SIZE)

    if process_type >= 0:
      process_data(trip['batch_id'], ordered_jsons, process_type, CHUNK_SIZE)


  list_rds_trips(limit=5)
  
  print("\n-- Merger Finished --\n")

if __name__ == "__main__":
    init_merger()