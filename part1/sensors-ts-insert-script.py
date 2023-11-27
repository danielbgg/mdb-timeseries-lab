from datetime import datetime, timedelta, tzinfo, timezone
import pandas
import pymongo
import time
from faker import Faker

import config

faker = Faker()
client = pymongo.MongoClient(config.CONNECTION_STRING)
ts_collection = client[config.DB_NAME][config.TS_COLL_NAME]

series = pandas.date_range(start=config.DATE_RANGE_START, end=config.DATE_RANGE_END, freq=config.DATE_RANGE_FREQUENCY) 
data_points = []
counter = 0
data_points_inserted = 0
start = time.time()

while (counter < config.SENSORS_COUNT):
    sensor_id = faker.random_int(min=1000, max=3999)
    truck_id = faker.uuid4()
    print(f'started populating time series data for sensor ID {sensor_id}')

    for d in series:
        doc = {
                "metadata": {
                    "sensor_id": sensor_id,
                    "sensor_type": "cabin",
                    "truck_id": truck_id,
                    "temp_unit": "celcius"
                },
                "ts": d,
                "temperature": faker.pyfloat(right_digits=1, min_value=0, max_value=3),
                "gps_location": [faker.pyfloat(right_digits=6, min_value=-180, max_value=180), faker.pyfloat(right_digits=6, min_value=-90, max_value=90)]
        }

        data_points.append(doc)

        # Insert small batches. This is done mostly to show progress while executing
        if(len(data_points) == config.BATCH_SIZE):
            ts_collection.insert_many(data_points)
            data_points_inserted += len(data_points)
            data_points = []
            print(f'{"{:,}".format(data_points_inserted)} data points inserted for sensor ID {sensor_id}')

    # Make sure that all data points are inserted
    if(len(data_points) > 0):
        ts_collection.insert_many(data_points, ordered=False)
        data_points_inserted += len(data_points)
        data_points = []
        print(f'{"{:,}".format(data_points_inserted)} data points inserted for sensor ID {sensor_id}')
    
    counter += 1

end = time.time()
diff = end - start
rate = round(data_points_inserted/diff)

print(f'finished inserting in {round(diff)} seconds, at a rate of {rate} data points per second')