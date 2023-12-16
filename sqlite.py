import sqlite3 as lite
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


lite_time = [[],[],[],[]]
tiny = open('C:\\folder\\bd\\lab3\\nyc_yellow_big.csv')
tiny.readline()
for_create_table = f"CREATE TABLE IF NOT EXISTS trips ({', '.join(columns)})"
with lite.connect(':memory:') as conn:
    cur = conn.cursor()
    for_create_table = f"CREATE TABLE IF NOT EXISTS trips ({', '.join(columns)})"
    cur.execute(for_create_table)
    for line in tiny:
        cur.execute('INSERT INTO trips VALUES(?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?)', line.split(','))
    for i in range(10):
        start = time.time()
        cur.execute('SELECT VendorID, count(*) FROM trips GROUP BY 1;')
        end = time.time()
        lite_time[0].append(end - start)
        start = time.time()
        cur.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
        end = time.time()
        lite_time[1].append(end - start)
        start = time.time()
        cur.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;")
        end = time.time()
        lite_time[2].append(end - start)
        start = time.time()
        cur.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;")
        end = time.time()
        lite_time[3].append(end - start)
tiny.close()
print(*lite_time[0])
print(*lite_time[1])
print(*lite_time[2])
print(*lite_time[3])