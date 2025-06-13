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
import mmap
from datetime import datetime
from utils import collect_utils
from utils import partition_utils
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import queue
import threading
import multiprocessing as mp


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

    conn, partition_fn_times = partition_utils.partition_and_distribute_streaming_parquet(comm, rank, size, 'customer', 'orders', 
                                              'c_custkey', 'o_custkey',
                                              db_path='data/coordinator/full_data/whole_tpch_0.1.duckdb')
    

    # synchronize all processes after partitioning and distributing
    # comm.Barrier()
    local_partition_time = datetime.now() - partition_start_time 
   

    # collect results

    query1 = f"""
    SELECT
        c.c_custkey,
        c.c_name,
        o.o_orderkey,
        o.o_orderdate,
        o.o_totalprice
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    """
    collection_start_time = datetime.now()
    
    # local_join_time, collection_time = collect_utils.collect_results_parquet_streaming_1(comm, rank, size, conn)
    local_join_time, collection_time = collect_utils.collect_results_streaming_parquet(
                                       comm, rank, size, conn, query1, 'customer_orders_results.parquet')
    # collection_time_2 = datetime.now() - collection_start_time
    
    total_time = datetime.now() - total_start_time 
    # find the max local_join_time across nodes
    max_partition_time = comm.reduce(local_partition_time, op=MPI.MAX, root=0)
    max_local_join_time = comm.reduce(local_join_time, op=MPI.MAX, root=0)

    
    # cleanup
    cleanup(rank)

    # total_time = datetime.now() - total_start_time 
    if rank == 0:
        print(f"total_time: {total_time.total_seconds()} seconds")
        print(f"max_partition_phase_time: {max_partition_time.total_seconds()} seconds")
        print(f"partition_time: {local_partition_time.total_seconds()} seconds")
        print(f"coord_partition_time: {partition_fn_times['partitioning_time']} seconds")
        print(f"partition_communication_time: {partition_fn_times['communication_time']} seconds")
        print(f"max_local_join_time: {max_local_join_time.total_seconds()} seconds")
        print(f"collection_time: {collection_time.total_seconds()} seconds")
        # print(f"Collection time 2: {collection_time_2.total_seconds()} seconds")

if __name__ == "__main__":
    main()