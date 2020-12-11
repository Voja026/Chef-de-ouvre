import psycopg2
from psycopg2 import OperationalError as e
from psycopg2 import sql

# dans le terminal : psql -d postgres
# CREATE DATABASE test;

def connect_db(db, user, password, host, port):
    try:
        connection = psycopg2.connect(database=db,user=user,password=password,host=host,port=port)
        print(f'Logged in {db} successfully !')
    except e:
        print(f'Could not log in to {db} !')
    return connection

conn = connect_db("plants", "voja", "postgres", "localhost", "5432")
cursor = conn.cursor()
conn.autocommit = True

def create_tables():
    try:
        capacity= '''CREATE TABLE IF NOT EXISTS capacities (
            id_capacity SERIAL PRIMARY KEY,
            capacity FLOAT8
            );'''
        localisation = '''CREATE TABLE IF NOT EXISTS localisations (
            id_localisation SERIAL PRIMARY KEY,
            country_name TEXT
            );'''
        fuel_type = '''CREATE TABLE IF NOT EXISTS fuel_types (
            id_fuel SERIAL PRIMARY KEY,
            fuel_name TEXT 
            );'''
        owner = '''CREATE TABLE IF NOT EXISTS owners (
            id_owner SERIAL PRIMARY KEY,
            owner_name TEXT
            );'''
        geolocalisation = '''CREATE TABLE IF NOT EXISTS geolocalisations (
          id_geo SERIAL PRIMARY KEY,
          latitude FLOAT8,
          longitude FLOAT8
        );'''
        power_plant_names = '''CREATE TABLE IF NOT EXISTS power_plant_names (
            id_name SERIAL PRIMARY KEY,
            name TEXT,
            commissioning_year INTEGER,
            id_geo INTEGER,
            FOREIGN KEY(id_geo) REFERENCES geolocalisations(id_geo)
            );'''
        power_plants = '''CREATE TABLE IF NOT EXISTS power_plants (
          id_power_plant SERIAL PRIMARY KEY,
          id_name INTEGER,
          id_capacity INTEGER,
          id_localisation INTEGER,
          id_fuel INTEGER,
          id_owner INTEGER,
          FOREIGN KEY(id_name) REFERENCES power_plant_names(id_name),
          FOREIGN KEY(id_capacity) REFERENCES capacities(id_capacity),
          FOREIGN KEY(id_localisation) REFERENCES localisations(id_localisation),
          FOREIGN KEY(id_fuel) REFERENCES fuel_types(id_fuel),
          FOREIGN KEY(id_owner) REFERENCES owners(id_owner)
        );'''    
        
        cursor.execute(capacity)
        cursor.execute(localisation)
        cursor.execute(fuel_type)
        cursor.execute(owner)
        cursor.execute(geolocalisation)
        cursor.execute(power_plant_names)
        cursor.execute(power_plants)
        
        print("Tables created successfully!")
    except e:
        print("Could not create tables")

# def create_casino(nom):
#     try:
#         query_name = '''INSERT INTO casinos (nom) VALUES (%s);'''
#         cursor.execute(query_name, (nom,))
#         print(f'Le casino {nom} a bien été ajouté!')
#     except psycopg2.Error as error:
#         print(f'{error.args}')
#     # finally:
#     #     conn.commit()

# def create_croupier(nom, casino_id):
#     try:
#         query_name = '''INSERT INTO croupiers (nom, casino_id) VALUES (%s, %s);'''
#         cursor.execute(query_name, (nom, casino_id))
#         print(f'Le croupier {nom} a bien été ajouté!')
#     except psycopg2.Error as error:
#         print(f'{error.args}')
#     # finally:
#     #     conn.commit()
