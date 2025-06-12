# generate_tpch.py

import os
import duckdb

# Create output directory
os.makedirs('data/coordinator/full_data', exist_ok=True)
con = duckdb.connect("data/coordinator/full_data/whole_tpch_5.duckdb")

# Install and load TPC-H extension
con.execute("INSTALL tpch")
con.execute("LOAD tpch")

# Generate the data
con.execute("CALL dbgen(sf=5)")

# Show results
tables = con.execute("SHOW TABLES").fetchall()
print("[Coordinator] Created tables:", ", ".join([t[0] for t in tables]))

orders_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
print(f"Orders records: {orders_count}")

customer_count = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
print(f"Customer records: {customer_count}")

con.close()
print("[Coordinator] Database file created successfully!")
