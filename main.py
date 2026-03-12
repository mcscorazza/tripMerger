from database import *
from dynamo import search_pending_trips

def init_merger():
    print("Starting the Merger...\n")
    pending_trips = search_pending_trips()
    
    if not pending_trips:
        print("No pending trips to process. Going back to sleep.")
    
    for trip in pending_trips:
        print("------------------------------------------------------------")
        print("- Batch ID: ", trip['batch_id'])
    print("------------------------------------------------------------")
    
if __name__ == "__main__":
    init_merger()