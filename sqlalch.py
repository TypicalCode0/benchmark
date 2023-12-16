import sqlalchemy as al
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, Session
import time 
import datetime

al_time = [[],[],[],[]]
class Base(DeclarativeBase):
    pass

base = DeclarativeBase()
engine = al.create_engine('sqlite:///:memory:', echo=True)
class trips(Base):
    __tablename__ = "trips"
    id: Mapped[int] = mapped_column(primary_key=True)
    VendorID: Mapped[int] = mapped_column()
    tpep_pickup_datetime = mapped_column(al.DateTime)
    tpep_dropoff_datetime = mapped_column(al.DateTime)
    passenger_count: Mapped[float] = mapped_column()
    trip_distance: Mapped[float] = mapped_column()
    RatecodeID: Mapped[float] = mapped_column()
    store_and_fwd_flag: Mapped[str] = mapped_column()
    PULocationID: Mapped[int] = mapped_column()
    DOLocationID: Mapped[int] = mapped_column()
    payment_type: Mapped[int] = mapped_column()
    fare_amount: Mapped[float] = mapped_column()
    extra: Mapped[float] = mapped_column()
    mta_tax: Mapped[float] = mapped_column()
    tip_amount: Mapped[float] = mapped_column()
    tolls_amount: Mapped[float] = mapped_column()
    improvement_surcharge: Mapped[float] = mapped_column()
    total_amount: Mapped[float] = mapped_column()
    congestion_surcharge: Mapped[float] = mapped_column()
    airport_fee: Mapped[float] = mapped_column(nullable=True)

def ffloat(s):
    if s == '':
        return 0
    else:
        return float(s)

tiny = open('C:\\folder\\bd\lab3\\tiny_data.csv')
tiny.readline()
Base.metadata.create_all(bind=engine)
with Session(autoflush=False, bind=engine) as session:
    for line in tiny:
        line = line.split(',')
        add = trips(
            id = int(line[0]),
            VendorID = int(line[1]),
            tpep_pickup_datetime = datetime.datetime.strptime(line[2],'%Y-%m-%d %H:%M:%S'),
            tpep_dropoff_datetime = datetime.datetime.strptime(line[3],'%Y-%m-%d %H:%M:%S'),
            passenger_count = ffloat(line[4]),
            trip_distance = ffloat(line[5]),
            RatecodeID = ffloat(line[6]),
            store_and_fwd_flag = line[7],
            PULocationID = int(line[8]),
            DOLocationID = int(line[9]),
            payment_type = int(line[10]),
            fare_amount = ffloat(line[11]),
            extra = ffloat(line[12]),
            mta_tax = ffloat(line[13]),
            tip_amount = ffloat(line[14]),
            tolls_amount = ffloat(line[15]),
            improvement_surcharge = ffloat(line[16]),
            total_amount = ffloat(line[17]),
            congestion_surcharge = ffloat(line[18]),
            airport_fee = ffloat(line[19]),
        )
        session.add(add)
    session.commit()
    for i in range(10):
        start = time.time()
        q1 = session.execute(al.select(trips.VendorID, al.func.count("*")).group_by(trips.VendorID))
        end = time.time()
        al_time[0].append(end - start)
        start = time.time()
        q2 = session.execute(al.select(trips.passenger_count, al.func.avg(trips.total_amount)).group_by(trips.passenger_count))
        end = time.time()
        al_time[1].append(end - start)
        start = time.time()
        q3 = session.execute(al.select(trips.passenger_count.label('1'), al.func.strftime('%Y', trips.tpep_pickup_datetime).label('2'), al.func.count("*").label('3')).group_by('1','2'))
        end = time.time()
        al_time[2].append(end - start)
        start = time.time()
        q4 = session.execute(al.select(trips.passenger_count.label('1'), al.func.strftime('%Y', trips.tpep_pickup_datetime).label('2'), al.func.round(trips.trip_distance).label('3'), al.func.count("*").label('4')).group_by('1', '2', '3').order_by('2', al.func.count("*").desc()))
        end = time.time()
        al_time[3].append(end - start)
    session.close_all()
    engine.dispose()
tiny.close()
print(*al_time[0])
print(*al_time[1])
print(*al_time[2])
print(*al_time[3])