from database import *
from s3 import *
from dynamo import search_pending_trips

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
        if len(jsons_files) < CHUNK_SIZE:
          print(f" > Number of files less then {CHUNK_SIZE}", end="")
          if last_trip_status(jsons_files):
            print(f", but is FINISH chunk!")
            

    print("\n-- Merger Finished --\n")

if __name__ == "__main__":
    init_merger()