from database import list_rds_trips
from dynamo import search_pending_trips
from s3 import get_s3_objects, chunk_type_optimized, print_s3_list
from merger import process_data

CHUNK_SIZE = 600

def init_merger():
  print("-- Starting the Merger --\n")

  pending_trips = search_pending_trips()

  if pending_trips:
    print_s3_list(pending_trips)

  for trip in pending_trips:
    batch_id = trip['batch_id']
    print(f"\nGet infos for trip: {batch_id}")
    
    jsons_files = get_s3_objects(batch_id)
    if not jsons_files:
      continue
        
    ordered_jsons = sorted(jsons_files)
    process_type = chunk_type_optimized(ordered_jsons, CHUNK_SIZE)

    if process_type >= 0:
      process_data(batch_id, ordered_jsons, process_type, CHUNK_SIZE)

  list_rds_trips(limit=5)
  print("\n-- Merger Finished --\n")

if __name__ == "__main__":
  init_merger()