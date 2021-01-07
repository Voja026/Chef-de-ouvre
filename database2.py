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

conn = connect_db("plants2", "energy", "ecolo", "localhost", "5432")
cursor = conn.cursor()
conn.autocommit = True

def create_tables():
        query_pp = """CREATE TABLE "power_plants" (
	"id" serial NOT NULL,
	"name" TEXT NOT NULL,
	"capacity" FLOAT NOT NULL,
	"commissioning_year" FLOAT NOT NULL,
	"lat" FLOAT NOT NULL,
	"long" FLOAT NOT NULL,
	"id_country" integer NOT NULL,
	"fuel" TEXT NOT NULL,
	CONSTRAINT "power_plants_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);"""


        query_country = """
                CREATE TABLE "countries" (
                "id" serial NOT NULL,
                "name" TEXT NOT NULL UNIQUE,
                CONSTRAINT "countries_pk" PRIMARY KEY ("id")
            ) WITH (
              OIDS=FALSE
);"""


        query_eco = """
        CREATE TABLE "economics" (
	"id_eco" serial NOT NULL,
	"population" FLOAT NOT NULL,
	"gdp_usd" FLOAT NOT NULL,
	"id_country" integer NOT NULL,
	CONSTRAINT "economics_pk" PRIMARY KEY ("id_eco")
) WITH (
  OIDS=FALSE
);"""

        query_emissions = """

CREATE TABLE "emissions" (
	"id_emission" serial NOT NULL,
	"co2" FLOAT NOT NULL,
	"id_country" integer NOT NULL,
	CONSTRAINT "emissions_pk" PRIMARY KEY ("id_emission")
) WITH (
  OIDS=FALSE
);"""
        query_alter_table = """ ALTER TABLE "economics" ADD CONSTRAINT "economics_fk0" FOREIGN KEY ("id_country") REFERENCES "countries"("id");
    ALTER TABLE "emissions" ADD CONSTRAINT "emissions_fk0" FOREIGN KEY ("id_country") REFERENCES "countries"("id");"""

        cursor.execute(query_pp)
        print('power_plants table created!')
        cursor.execute(query_country)
        print('countries table created!')
        cursor.execute(query_eco)
        print('economics table created!')
        cursor.execute(query_emissions)
        print('emissions  table created!')
        cursor.execute(query_alter_table)
        print('tables altered')
