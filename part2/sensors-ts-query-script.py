from datetime import datetime, timedelta, tzinfo, timezone
import json
import pymongo
import time

import config

client = pymongo.MongoClient(config.CONNECTION_STRING)
ts_collection = client[config.DB_NAME][config.COLL_NAME]

data_points = []
start = time.time()

# TODO INSERT QUERY LOGIC
data_points = ts_collection.find({"sensor_id": 3224})

print(json.dumps(data_points.collection.find_one(), indent=4, default=str))

end = time.time()
diff = end - start

print(f'finished querying in {round(diff*1000)} milliseconds')