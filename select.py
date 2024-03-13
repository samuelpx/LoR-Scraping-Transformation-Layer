#!/usr/bin/env python3

import duckdb

con = duckdb.connect("database.db")

con.sql("SELECT * FROM lor_scraping_t ORDER BY id DESC LIMIT 10").show()
