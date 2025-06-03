#!/usr/bin/env python3
"""
Upload DuckDB to node 0 and partition to other nodes
"""
from mpi4py import MPI
import duckdb
import os
import time
import sys
from datetime import datetime

def partition_and_distribute(rank, size, comm, table_name):
    """
    Node 0 loads the TPC-H data, partitions the {table_name} table based on custkey % size
    Distributes data partitions to all nodes.
    """
    pass

def partition_and_distribute_customers(comm, rank, size):
    """
    Node 0 loads the TPC-H data, partitions the customer table based on custkey % size
    Distributes data partitions to all nodes.
    """
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/customer_node_{rank}.duckdb'
    local_parquet_path = f'/tmp/customer_local.parquet'

    if rank == 0:
        # coordinator node
        print("Rank 0: Partitioning and distributing into Parquet files")

        con = duckdb.connect(db_path)

        for i in range(size):
            # loop over the nodes
            part_path = f'/tmp/customer_part_{i}.parquet'
            con.execute(f"""
            COPY (SELECT * FROM customer where c_custkey % {size} = {i}) 
            TO '{part_path}' (FORMAT 'parquet')""")

            if i == 0:
                # don't need to send to self
                continue

            # read file and send bytes
            with open(part_path, 'rb') as f:
                file_bytes = f.read()

            comm.send(file_bytes, dest=i, tag=77)
            print(f"Rank 0: Sent partition {i} to rank {i}")
        
        con.close()
    
    else:
        # receive the parquet file bytes
        print(f"Rank {rank}: Waiting to receive Parquet file...")
        file_bytes = comm.recv(source=0, tag=77)

        # Save to local file
        with open(local_parquet_path, 'wb') as f:
            f.write(file_bytes)

        print(f"Rank {rank}: Received and wrote Parquet file.")

    # Load local Parquet file into DuckDB for each rank
    con = duckdb.connect(local_db_path)
    if rank == 0:
        local_parquet_path = '/tmp/customer_part_0.parquet'

    con.execute("""
        CREATE TABLE customer AS
        SELECT * FROM read_parquet(?)
    """, (local_parquet_path,))

    count = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
    print(f"Rank {rank}: Loaded {count} customer records into local DuckDB.")

def test_customer_data_partitioning_count(rank, size):
    """
    For a given rank, output count of how many rows in this table. 
    """
    pass

def test_customer_data_partitioning_show_keys(rank, size):
    """
    For a given rank, output the first 3 c_custkeys
    """
    local_db_path = f'/tmp/customer_node_{rank}.duckdb'

    con = duckdb.connect(local_db_path)

    keys = con.execute("""
        SELECT c_custkey FROM customer LIMIT 3""").fetchall()

    # print(f"Rank {rank}: First 3 customer keys {', '.join([k[0] for k in keys])}")
    result = f"First 3 customer keys for rank {rank}: {', '.join([str(k[0]) for k in keys])}"
    print(result)
    return result

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        print("=" * 60)
        print(f"DuckDB + MPI Partitioning Test on {size} nodes")
        print("=" * 60)

    partition_and_distribute_customers(comm, rank, size)

    print("Finished partitioning and distributing; displaying first 3 of each node")
    local_result = test_customer_data_partitioning_show_keys(rank, size)

    # gather results from all ranks
    all_results = comm.gather(local_result, root=0)

    if rank == 0:
        # only the coordinator node prings
        print("\nTest: Customer data partitioning show keys")
        print("-" * 40)

        for i, result in enumerate(all_results):
            
            if result:
                print(result)
            else:
                print("Rank failed")
            

if __name__ == "__main__":
    main()