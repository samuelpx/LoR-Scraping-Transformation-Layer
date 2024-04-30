#!/usr/bin/python3

import duckdb

#Initiate the db if it does not exist, create a cursor otherwise.
con = duckdb.connect("database.db")

#Initiating a sequence in order for the primary key of our table to auto-increment
try:
    con.sql("CREATE SEQUENCE id_sequence START 1")
except duckdb.duckdb.CatalogException:
    print("Sequence for Primary key already exists")

#Since we are creating the whole table from a monolithic parquet file
#we need to make sure the table is drop to avoid redundancy
try:
    con.sql("DROP TABLE lor_scraping_t")
except duckdb.duckdb.CatalogException:
    print("Table did not exist, initializing table")

#Creating our table, setting data-types 
#adding the auto-incrementable primary key
con.sql("CREATE TABLE lor_scraping_t (id INT DEFAULT nextval('id_sequence') PRIMARY KEY, name VARCHAR, rank INT64, lp DOUBLE, date VARCHAR)")

#Inserting data from a direct query to the parquet file
con.sql("INSERT INTO lor_scraping_t (name, rank, lp, date) select name, rank, lp, date FROM read_parquet('/home/samuelpx/Documents/Projects/python/LoR-Scraping/transformed_data.parquet')")
