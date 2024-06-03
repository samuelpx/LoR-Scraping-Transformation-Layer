#!/usr/bin/env python3

import duckdb

con = duckdb.connect("database.db")

con.sql("SELECT * FROM lor_scraping_t ORDER BY id DESC LIMIT 10").show()
con.sql("SELECT DISTINCT date from lor_scraping_t ORDER BY date DESC").show()
