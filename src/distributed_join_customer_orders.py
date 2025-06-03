#!/usr/bin/env python3
"""
Perform simple query for customer and orders table
SELECT * FROM customer INNER JOIN orders ON customer.c_custkey=orders.o_custkey
"""
from mpi4py import MPI
import duckdb
import os
import time
import sys
from datetime import datetime

# def partition_and_distribute_customer_and_orders(comm, rank, size):
#     """
#     Node 0 loads the TPC-H data, partitions the customer and orders tables based on custkey % size
#     Distributes data partitions to all nodes.

#     Return the local duckdb connection.

#     Node 0 loads the TPC-H data, partitions the tables based on custkey % size
#     Distributes data partitions to all nodes.
#     """
#     db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
#     local_db_path_c = f'/tmp/customer_node_{rank}.duckdb'
#     local_parquet_path_c = f'/tmp/customer_local.parquet'
#     local_db_path_o = f'/tmp/orders_node_{rank}.duckdb'
#     local_parquet_path_o = f'/tmp/orders_local.parquet'

#     if rank == 0:
#         # coordinator node
#         print("Rank 0: Partitioning and distributing into Parquet files")

#         con = duckdb.connect(db_path)

#         for i in range(size):
#             # loop over the nodes
#             part_path_c = f'/tmp/customer_part_{i}.parquet'
#             part_path_o = f'/tmp/orders_part_{i}.parquet'
#             con.execute(f"""
#             COPY (SELECT * FROM customer where c_custkey % {size} = {i}) 
#             TO '{part_path_c}' (FORMAT 'parquet')""")
            
#             con.execute(f"""COPY (SELECT * FROM orders where o_custkey % {size} = {i}) 
#             TO '{part_path_o}' (FORMAT 'parquet')""")

#             if i == 0:
#                 # don't need to send to self
#                 continue

#             # read file and send bytes
#             with open(part_path_c, 'rb') as f:
#                 file_bytes_c = f.read()
#             with open(part_path_o, 'rb') as f:
#                 file_bytes_o = f.read()

#             comm.send(file_bytes_c, dest=i, tag=77)
#             comm.send(file_bytes_o, dest=i, tag=77)
#             print(f"Rank 0: Sent partition {i} to rank {i}")
        
#         con.close()
    
#     else:
#         # receive the parquet file bytes
#         print(f"Rank {rank}: Waiting to receive Parquet file...")
#         file_bytes = comm.recv(source=0, tag=77)

#         # Save to local file
#         with open(local_parquet_path, 'wb') as f:
#             f.write(file_bytes)

#         print(f"Rank {rank}: Received and wrote Parquet file.")

#     # Load local Parquet file into DuckDB for each rank
#     con = duckdb.connect(local_db_path)
#     if rank == 0:
#         local_parquet_path = '/tmp/customer_part_0.parquet'

#     con.execute("""
#         CREATE TABLE customer AS
#         SELECT * FROM read_parquet(?)
#     """, (local_parquet_path,))

#     count = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
#     print(f"Rank {rank}: Loaded {count} customer records into local DuckDB.")

def partition_and_distribute_customer_and_orders(comm, rank, size):
    """
    Node 0 loads the TPC-H data, partitions the customer and orders tables based on c_custkey % size,
    and distributes Parquet partitions to all nodes.

    Each node receives their partitions and loads both into a single DuckDB file.

    Returns:
        duckdb.Connection: connection to the local DuckDB with customer and orders tables.
    """
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    if rank == 0:
        # Coordinator node
        print("Rank 0: Partitioning and distributing customer and orders Parquet files")

        con = duckdb.connect(db_path)

        for i in range(size):
            part_path_c = f'/tmp/customer_part_{i}.parquet'
            part_path_o = f'/tmp/orders_part_{i}.parquet'

            con.execute(f"""
                COPY (
                    SELECT * FROM customer WHERE c_custkey % {size} = {i}
                ) TO '{part_path_c}' (FORMAT 'parquet')
            """)

            con.execute(f"""
                COPY (
                    SELECT * FROM orders WHERE o_custkey % {size} = {i}
                ) TO '{part_path_o}' (FORMAT 'parquet')
            """)

            if i == 0:
                continue  # Coordinator doesn't need to send to itself

            # Send both Parquet files
            with open(part_path_c, 'rb') as f:
                comm.send(f.read(), dest=i, tag=100)
            with open(part_path_o, 'rb') as f:
                comm.send(f.read(), dest=i, tag=101)

            print(f"Rank 0: Sent customer and orders partitions to rank {i}")

        con.close()

    else:
        # Worker nodes receive Parquet files
        print(f"Rank {rank}: Receiving customer and orders Parquet files...")
        file_bytes_c = comm.recv(source=0, tag=100)
        file_bytes_o = comm.recv(source=0, tag=101)

        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)

        print(f"Rank {rank}: Received and saved Parquet files")

    # Load into one local DuckDB file
    if rank == 0:
        # Coordinator reads local partitions
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)

    con.execute("CREATE TABLE customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))

    # Verify counts
    count_c = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
    count_o = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    print(f"Rank {rank}: Loaded {count_c} customer and {count_o} orders records into local DB")

    return con


def perform_local_join(rank, conn):
    """
    Returns list of tuples; maybe change to parquet file later on? 
    Check this: MPI needs to send serializable data.
    """
    start_time = datetime.now()
    query = """SELECT
                    c.c_custkey,
                    c.c_name,
                    o.o_orderkey,
                    o.o_orderdate,
                    o.o_totalprice
                FROM
                    customer c
                JOIN
                    orders o
                ON
                    c.c_custkey = o.o_custkey"""
    results = conn.execute(query).fetchall()
    elapsed_time = datetime.now() - start_time 

    print(f"Rank {rank}: Local join completed in {elapsed_time}, found {len(results)} results")
    print(f"\tFirst 5 rows are {results[:5]}")
    return results 


def collect_results(comm, rank, size, local_results):
    all_results = comm.gather(local_results, root=0)

    if rank == 0:
        print("Collecting and merging results...")

        merged_results = []
        for result in all_results:
            merged_results.extend(result)

         # Create DuckDB connection
        con = duckdb.connect()

        # Create a table from the result list
        con.execute("""
            CREATE TABLE join_result (
                c_custkey INTEGER,
                c_name TEXT,
                o_orderkey INTEGER,
                o_orderdate DATE,
                o_totalprice DOUBLE
            )
        """)
        con.executemany("""
            INSERT INTO join_result VALUES (?, ?, ?, ?, ?)
        """, merged_results)

        # Write to Parquet file
        output_path = 'data/joined_results.parquet'
        con.execute(f"""
            COPY join_result TO '{output_path}' (FORMAT 'parquet')
        """)
        print(f"Saved merged results to {output_path}")

        con.close()

def cleanup(rank):
    """
    Clean up temporary files (local db? join intermediate results)
    """
    pass 

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        print("=" * 60)
        print(f"Distributed join customer and orders tables on {size} nodes")
        print("=" * 60)

    # partition the customer table and the orders table
    conn = partition_and_distribute_customer_and_orders(comm, rank, size)

    # synchronize all processes after partitioning and distributing
    comm.Barrier()

    # perform local join
    local_results = perform_local_join(rank, conn)

    # collect results
    collect_results(comm, rank, size, local_results)

    # cleanup
    

if __name__ == "__main__":
    main()