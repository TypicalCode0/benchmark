import duckdb as dk
import time 

columns = [
        "ID INTEGER PRIMARY KEY",
        "VendorID INTEGER",
        "tpep_pickup_datetime timestamp",
        "tpep_dropoff_datetime timestamp",
        "passenger_count FLOAT",
        "trip_distance FLOAT",
        "RatecodeID FLOAT",
        "store_and_fwd_flag TEXT",
        "PULocationID INTEGER",
        "DOLocationID INTEGER",
        "payment_type INTEGER",
        "fare_amount FLOAT8",
        "extra FLOAT8",
        "mta_tax FLOAT8",
        "tip_amount FLOAT8",
        "tolls_amount FLOAT8",
        "improvement_surcharge FLOAT8",
        "total_amount FLOAT8",
        "congestion_surcharge FLOAT8",
        "airport_fee FLOAT8",
        "another_airport_fee FLOAT8",
]


duck_time = [[],[],[],[]]
for_create_table = f"CREATE TABLE IF NOT EXISTS trips ({', '.join(columns)})"
conn = dk.connect(database=':memory:', read_only=False)
conn.execute(for_create_table)
conn.execute("COPY trips from 'C:\\folder\\bd\lab3\\nyc_yellow_big.csv' (DELIMITER ',', HEADER)")
for i in range(10):
    start = time.time()
    records = conn.execute('SELECT VendorID, count(*) FROM trips GROUP BY 1;')
    end = time.time()
    duck_time[0].append(end - start)
    start = time.time()
    records = conn.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
    end = time.time()
    duck_time[1].append(end - start)
    start = time.time()
    records = conn.execute('SELECT passenger_count, extract(YEAR from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;')
    end = time.time()
    duck_time[2].append(end - start)
    start = time.time()
    records = conn.execute("SELECT passenger_count, extract(YEAR from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;")
    end = time.time()
    duck_time[3].append(end - start)
conn.close()
print(*duck_time[0])
print(*duck_time[1])
print(*duck_time[2])
print(*duck_time[3])