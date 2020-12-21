import psycopg2
from psycopg2 import OperationalError as e
from psycopg2 import sql
import csv

# dans le terminal : psql -d postgres
# CREATE DATABASE test;

def connect_db(db, user, password, host, port):
    try:
        connection = psycopg2.connect(database=db,user=user,password=password,host=host,port=port)
        print(f'Logged in {db} successfully !')
    except e:
        print(f'Could not log in to {db} !')
    return connection

conn = connect_db("plants", "energy", "ecolo", "localhost", "5432")
cursor = conn.cursor()
conn.autocommit = True

def create_tables():
    try:
        query_cap = '''CREATE TABLE IF NOT EXISTS capacities (
            id_cap SERIAL PRIMARY KEY,
            capacity FLOAT8
            );'''
        query_own = '''CREATE TABLE IF NOT EXISTS owners (
            id_owner SERIAL PRIMARY KEY,
            owner_name TEXT
            );'''
        query_fuel = '''CREATE TABLE IF NOT EXISTS fueltypes (
            id_fuel SERIAL PRIMARY KEY,
            fuel_type TEXT
            );'''
        query_loc = '''CREATE TABLE IF NOT EXISTS localisations (
            id_loc SERIAL PRIMARY KEY,
            country_name TEXT
            );'''
        query_geo = '''CREATE TABLE IF NOT EXISTS geolocalisations (
            id_geo SERIAL PRIMARY KEY,
            latitude FLOAT8,
            longitude FLOAT8
            );'''
        query_ppn = '''CREATE TABLE IF NOT EXISTS ppnames (
            id_ppn SERIAL PRIMARY KEY,
            name TEXT,
            commissioning_year TIMESTAMP,
            id_geo INTEGER,
            FOREIGN KEY(id_geo) REFERENCES geolocalisations(id_geo)
            );'''
        # query_pow = '''CREATE TABLE IF NOT EXISTS powerplants (
        #     id SERIAL PRIMARY KEY,
        #     id_name INTEGER,
        #     id_capacity INTEGER,
        #     id_localisation INTEGER,
        #     id_owner INTEGER,
        #     id_fuel INTEGER,
        #     FOREIGN KEY(id_name) REFERENCES ppnames(id_ppn),
        #     FOREIGN KEY(id_capacity) REFERENCES capacities(id_cap),
        #     FOREIGN KEY(id_localisation) REFERENCES localisations(id_loc),
        #     FOREIGN KEY(id_owner) REFERENCES owners(id_owner),
        #     FOREIGN KEY(id_fuel) REFERENCES fueltypes(id_fuel)
        #     );'''
        query_pow='''CREATE TABLE IF NOT EXISTS powerplants (
                id_cap integer references capacities,
                id_owner integer references owners,
                id_fuel integer references fueltypes,
                id_loc integer references localisations,
                id_ppn integer references ppnames,
                
                constraint pk primary key (id_cap ,id_owner, id_fuel,id_loc, id_ppn)
                );'''
                
        cursor.execute(query_cap)
        cursor.execute(query_own)
        cursor.execute(query_fuel)
        cursor.execute(query_loc)
        cursor.execute(query_geo)
        cursor.execute(query_ppn)
        cursor.execute(query_pow)
        print("Tables created successfully!")
    except e:
        print("Could not create tables")

def insert_cap():
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/capacities.csv'),delimiter=',')
        for row in data:
            cursor.execute("INSERT INTO capacities (capacity) VALUES (%s)",row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

def insert_own():
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/owner.csv'),delimiter=',')
        for row in data:
            cursor.execute("INSERT INTO owners (owner_name) VALUES (%s)",row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

def insert_fuel():
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/fuel.csv'),delimiter=',')
        for row in data:
            cursor.execute("INSERT INTO fueltypes (fuel_type) VALUES (%s)",row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

def insert_loc():
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/country.csv'),delimiter=',')
        for row in data:
            cursor.execute("INSERT INTO localisations (country_name) VALUES (%s)",row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

def insert_geo(latitude, longitude):
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/geo.csv'),delimiter=',')
        for row in data:
            cursor.execute('''INSERT INTO geolocalisations (latitude, longitude) VALUES (%s, %s);''', row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

def insert_ppn(name, commissioning_year):
    try:
        data = csv.reader(open('/home/voja/Desktop/Final project/ppnames.csv'),delimiter=',')
        for row in data:
            cursor.execute('''INSERT INTO ppnames (name, commissioning_year) VALUES (%s,%s);''', row)
        print("CSV data imported")
    except e:
        print("Could not insert data")

        
    #     def insert_pow():
    # # try:
    # #     data = csv.reader(open('/home/voja/Desktop/Final project/.csv'),delimiter=',')
    # #     for row in data:
    # #         cursor.execute('''INSERT INTO power_plants () VALUES (%s,%s);''', row)
    # #     print("CSV data imported")
    # # except e:
    # #     print("Could not insert data")