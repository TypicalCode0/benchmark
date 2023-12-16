import time 
import psycopg2 as ps


ps_time = [[], [], [], []]
tiny = open('C:\\folder\\bd\lab3\\nyc_yellow_big.csv')
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

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '12345678',
    'host': 'localhost',
    'port': '5432'
}
connection = ps.connect(**db_params)
cursor = connection.cursor()
cursor.execute('DROP TABLE if exists trips')
for_create_table = f"CREATE TABLE trips ({', '.join(columns)})"
cursor.execute(for_create_table)
cursor.copy_expert("COPY trips from STDIN WITH CSV HEADER", tiny)
for _ in range(10):
    start_time = time.time()
    cursor.execute('SELECT VendorID, count(*) FROM trips GROUP BY 1;')
    end_time = time.time()
    ps_time[0].append(end_time - start_time)
    start_time = time.time()
    cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;')
    end_time = time.time()
    ps_time[1].append(end_time - start_time)
    start_time = time.time()
    cursor.execute("SELECT passenger_count, extract(YEAR from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;")
    end_time = time.time()
    ps_time[2].append(end_time - start_time)
    start_time = time.time()
    cursor.execute("SELECT passenger_count, extract(YEAR from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;")
    end_time = time.time()
    ps_time[3].append(end_time - start_time)
cursor.close()
connection.close()
tiny.close()
print(*ps_time[0])
print(*ps_time[1])
print(*ps_time[2])
print(*ps_time[3])