import pandas as pd
import time 
import sqlalchemy as al

pd_time = [[],[],[],[]]
engine = al.create_engine('sqlite:///:memory:', echo=True)
tiny = open('C:\\folder\\bd\lab3\\nyc_yellow_big.csv')
data = pd.read_csv(tiny, delimiter=',')
df = pd.DataFrame(data)
df.to_sql('trips', con=engine, index=False)
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
q1 = 'SELECT VendorID, count(*) FROM trips GROUP BY 1;'
q2 = 'SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;'
q3 = "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS pickup_year, count(*) FROM trips GROUP BY 1, 2;"
q4 = "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS year, round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;"
for i in range(10):
    start = time.time()
    result = pd.read_sql(q1, con=engine)
    end = time.time()
    pd_time[0].append(end - start)
    start = time.time()
    result = pd.read_sql(q2, con=engine)
    end = time.time()
    pd_time[1].append(end - start)
    start = time.time()
    result = pd.read_sql(q3, con=engine)
    end = time.time()
    pd_time[2].append(end - start)
    start = time.time()
    result = pd.read_sql(q4, con=engine)
    end = time.time()
    pd_time[3].append(end - start)
engine.dispose()
tiny.close()
print(*pd_time[0])
print(*pd_time[1])
print(*pd_time[2])
print(*pd_time[3])