import duckdb
import pandas

con = duckdb.connect("database.db")

con.sql("CREATE SEQUENCE id_sequence START 1")

con.sql("DROP TABLE lor_scraping_t")

con.sql("CREATE TABLE lor_scraping_t (name VARCHAR, rank INT64, lp DOUBLE, date VARCHAR, id INT DEFAULT nextval('id_sequence') PRIMARY KEY)")

con.sql("INSERT INTO lor_scraping_t (name, rank, lp, date) select name, rank, lp, date FROM read_parquet('transformed_data.parquet')")

