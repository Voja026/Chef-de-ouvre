import database

#database.connect_db("test", "energy", "ecolo", "localhost", "5432")

database.create_tables()
# database.insert_geo("latitude", "longitude")
database.insert_ppn("name", "latitude","longitude","commissioning_year")
# database.insert_cap()
# database.insert_own()
# database.insert_fuel()
# database.insert_loc()