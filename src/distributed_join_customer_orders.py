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
from utils import collect_utils

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
        # print("Rank 0: Partitioning and distributing customer and orders Parquet files")

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

            # print(f"Rank 0: Sent customer and orders partitions to rank {i}")

        con.close()

    else:
        # Worker nodes receive Parquet files
        # print(f"Rank {rank}: Receiving customer and orders Parquet files...")
        file_bytes_c = comm.recv(source=0, tag=100)
        file_bytes_o = comm.recv(source=0, tag=101)

        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)

        # print(f"Rank {rank}: Received and saved Parquet files")

    # Load into one local DuckDB file
    if rank == 0:
        # Coordinator reads local partitions
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)

    con.execute("CREATE TABLE IF NOT EXISTS customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))

    # Verify counts
    count_c = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
    count_o = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    # print(f"Rank {rank}: Loaded {count_c} customer and {count_o} orders records into local DB")

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

    # print(f"Rank {rank}: Local join completed in {elapsed_time}, found {len(results)} results")
    # print(f"\tFirst 5 rows are {results[:5]}")
    return results 


def collect_results(comm, rank, size, local_results):
    all_results = comm.gather(local_results, root=0)

    if rank == 0:
        # print("Collecting and merging results...")

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
        # print(f"Saved merged results to {output_path}")

        con.close()

def collect_results_optimized(comm, rank, size, local_results):
    """
    Optimized collection phase with multiple strategies for better performance
    """
    if rank == 0:
        print("Starting optimized collection phase...")
        start_time = datetime.now()
    
    # Strategy 1: Use reduce instead of gather to avoid memory bottleneck
    # Only collect counts first to estimate total size
    local_count = len(local_results)
    total_count = comm.reduce(local_count, op=MPI.SUM, root=0)
    
    if rank == 0:
        print(f"Total records to collect: {total_count}")
    
    # Strategy 2: Stream results instead of collecting all at once
    if rank == 0:
        # Initialize output file directly
        output_path = 'data/joined_results.parquet'
        con = duckdb.connect()
        
        con.execute("""
            CREATE TABLE join_result (
                c_custkey INTEGER,
                c_name TEXT,
                o_orderkey INTEGER,
                o_orderdate DATE,
                o_totalprice DOUBLE
            )
        """)
        
        # Insert coordinator's own results first
        if local_results:
            con.executemany("""
                INSERT INTO join_result VALUES (?, ?, ?, ?, ?)
            """, local_results)
        
        # Receive and process results from other nodes one by one
        for source_rank in range(1, size):
            print(f"Receiving results from rank {source_rank}...")
            remote_results = comm.recv(source=source_rank, tag=200)
            
            if remote_results:
                # Insert in batches to avoid memory issues
                batch_size = 10000
                for i in range(0, len(remote_results), batch_size):
                    batch = remote_results[i:i + batch_size]
                    con.executemany("""
                        INSERT INTO join_result VALUES (?, ?, ?, ?, ?)
                    """, batch)
        
        # Write to Parquet file
        con.execute(f"""
            COPY join_result TO '{output_path}' (FORMAT 'parquet')
        """)
        
        elapsed_time = datetime.now() - start_time
        print(f"Collection completed in {elapsed_time}")
        print(f"Saved merged results to {output_path}")
        con.close()
        
    else:
        # Send results to coordinator
        comm.send(local_results, dest=0, tag=200)

# def collect_results_parquet_streaming(comm, rank, size, conn):
#     """
#     Alternative approach: Stream Parquet files instead of raw data
#     This avoids Python object serialization overhead
#     """
#     # Each node writes its results to a temporary Parquet file
#     temp_parquet = f'/tmp/join_results_rank_{rank}.parquet'
    
#     query = f"""
#     COPY (
#         SELECT
#             c.c_custkey,
#             c.c_name,
#             o.o_orderkey,
#             o.o_orderdate,
#             o.o_totalprice
#         FROM customer c
#         JOIN orders o ON c.c_custkey = o.o_custkey
#     ) TO '{temp_parquet}' (FORMAT 'parquet')
#     """
    
#     start_time = datetime.now()
#     conn.execute(query)
#     query_time = datetime.now() - start_time
    
#     # Get file size for monitoring
#     file_size = os.path.getsize(temp_parquet) if os.path.exists(temp_parquet) else 0
#     print(f"Rank {rank}: Query completed in {query_time}, wrote {file_size} bytes")
    
#     if rank == 0:
#         print("Coordinator collecting Parquet files...")
#         start_time = datetime.now()
        
#         # Create final database
#         final_conn = duckdb.connect()
        
#         # Read coordinator's own file first
#         final_conn.execute(f"""
#             CREATE TABLE join_result AS 
#             SELECT * FROM read_parquet('{temp_parquet}' )
#         """)
        
#         # Collect Parquet files from other nodes
#         for source_rank in range(1, size):
#             print(f"Receiving Parquet from rank {source_rank}...")
            
#             # Receive file bytes
#             file_bytes = comm.recv(source=source_rank, tag=300)
            
#             # Write to temporary file
#             remote_parquet = f'/tmp/join_results_from_rank_{source_rank}.parquet'
#             with open(remote_parquet, 'wb') as f:
#                 f.write(file_bytes)
            
#             # Append to main table using DuckDB's efficient Parquet reading
#             final_conn.execute(f"""
#                 INSERT INTO join_result 
#                 SELECT * FROM read_parquet('{remote_parquet}')
#             """)
            
#             # Clean up temporary file
#             os.remove(remote_parquet)
        
#         # Export final result
#         output_path = 'data/joined_results.parquet'
#         final_conn.execute(f"""
#             COPY join_result TO '{output_path}' (FORMAT 'parquet')
#         """)
        
#         elapsed_time = datetime.now() - start_time
#         final_count = final_conn.execute("SELECT COUNT(*) FROM join_result").fetchone()[0]
#         print(f"Collection completed in {elapsed_time}, total records: {final_count}")
        
#         final_conn.close()
        
#     else:
#         # Send Parquet file to coordinator
#         with open(temp_parquet, 'rb') as f:
#             file_bytes = f.read()
#         comm.send(file_bytes, dest=0, tag=300)
    
#     # Clean up local temporary file
#     if os.path.exists(temp_parquet):
#         os.remove(temp_parquet)

def cleanup(rank):
    """
    Clean up temporary files (local db? join intermediate results)
    """
    temp_files = [
        f'/tmp/customer_part_{rank}.parquet',
        f'/tmp/orders_part_{rank}.parquet',
        f'/tmp/customer_local_{rank}.parquet', 
        f'/tmp/orders_local_{rank}.parquet',
        f'/tmp/local_results_{rank}.parquet',
        f'/tmp/node_{rank}.duckdb'
    ]
    
    for file_path in temp_files:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except:
            pass

def main():

    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    total_start_time = datetime.now()

    if rank == 0:
        print("=" * 60)
        print(f"Distributed join customer and orders tables on {size} nodes")
        print("=" * 60)

    # partition the customer table and the orders table
    partition_start_time = datetime.now()
    conn = partition_and_distribute_customer_and_orders(comm, rank, size)

    # synchronize all processes after partitioning and distributing
    comm.Barrier()
    partition_time = datetime.now() - partition_start_time 

    # perform local join (built into parquet streaming)
    # local_join_start_time = datetime.now()
    # local_results = perform_local_join(rank, conn)
    # local_join_time = datetime.now() - local_join_start_time

    # collect results
    collection_start_time = datetime.now()
    # collect_results(comm, rank, size, local_results)
    # collect_results_optimized(comm, rank, size, local_results)
    # collect_results_parquet_streaming(comm, rank, size, conn)
    
    local_join_time, collection_time = collect_utils.collect_results_parquet_streaming(comm, rank, size, conn)
    collection_time_2 = datetime.now() - collection_start_time
    

    # find the max local_join_time across nodes
    max_local_join_time = comm.reduce(local_join_time, op=MPI.MAX, root=0)


    # cleanup
    cleanup(rank)

    total_time = datetime.now() - total_start_time 
    if rank == 0:
        print(f"total_time: {total_time.total_seconds()} seconds")
        print(f"partition_time: {partition_time.total_seconds()} seconds")
        print(f"max_local_join_time: {max_local_join_time.total_seconds()} seconds")
        print(f"collection_time: {collection_time.total_seconds()} seconds")
        print(f"Collection time 2: {collection_time_2.total_seconds()} seconds")

        # print(f"RESULT: total_time={total_time.total_seconds()} partition_time={partition_time.total_seconds()} local_join_time={join_time.total_seconds()} collection_time={collection_time.total_seconds()}")


if __name__ == "__main__":
    main()