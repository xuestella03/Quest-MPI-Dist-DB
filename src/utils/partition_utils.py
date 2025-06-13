from mpi4py import MPI
import duckdb
import os
import time
import sys
import mmap 
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import queue
import threading
import multiprocessing as mp


def partition_and_distribute_streaming_parquet(comm, rank, size, table1_name, table2_name, 
                                              table1_partition_key, table2_partition_key,
                                              db_path='data/coordinator/full_data/whole_tpch_5.duckdb'):
    """
    Partition and distribute two arbitrary tables across MPI ranks using streaming parquet files.
    
    Args:
        comm: MPI communicator
        rank: Current MPI rank
        size: Total number of MPI ranks
        table1_name: Name of the first table to partition
        table2_name: Name of the second table to partition
        table1_partition_key: Column name to use for partitioning table1
        table2_partition_key: Column name to use for partitioning table2
        db_path: Path to the source DuckDB database
    
    Returns:
        tuple: (local_connection, times) where times is a dictionary of total seconds
    """
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_1 = f'/tmp/{table1_name}_local.parquet'
    local_parquet_path_2 = f'/tmp/{table2_name}_local.parquet'

    # initialize the times (dictionary)
    times = {
        'partitioning_time': 0,
        'communication_time': 0,
    }
    
    if rank == 0:
        partitioning_start_time = datetime.now()
        con = duckdb.connect(db_path, read_only=True)
        pending_reqs = []
        
        for i in range(size):
            part_path_1 = f'/tmp/{table1_name}_part_{i}.parquet'
            part_path_2 = f'/tmp/{table2_name}_part_{i}.parquet'
            
            # Partition first table
            con.execute(f"""
                COPY (
                    SELECT * FROM {table1_name} WHERE {table1_partition_key} % {size} = {i}
                ) TO '{part_path_1}' (FORMAT 'parquet')
            """)
            
            # Partition second table
            con.execute(f"""
                COPY (
                    SELECT * FROM {table2_name} WHERE {table2_partition_key} % {size} = {i}
                ) TO '{part_path_2}' (FORMAT 'parquet')
            """)
            
            # Read partition files
            with open(part_path_1, 'rb') as f:
                file_bytes_1 = f.read()
            with open(part_path_2, 'rb') as f:
                file_bytes_2 = f.read()
            
            if i != 0:
                # Send sizes first
                size_1 = len(file_bytes_1)
                size_2 = len(file_bytes_2)
                req_size_1 = comm.isend(size_1, dest=i, tag=200)
                req_size_2 = comm.isend(size_2, dest=i, tag=201)
                
                # Send actual data
                req_data_1 = comm.Isend([file_bytes_1, MPI.BYTE], dest=i, tag=100)
                req_data_2 = comm.Isend([file_bytes_2, MPI.BYTE], dest=i, tag=101)
                
                pending_reqs.extend([req_size_1, req_size_2, req_data_1, req_data_2])
            
            # Clean up partition files for non-rank-0 nodes
            if i != 0:
                os.remove(part_path_1)
                os.remove(part_path_2)
        
        # finished partitioning; record time
        times['partitioning_time'] = (datetime.now() - partitioning_start_time).total_seconds()

        # Wait for all async sends to complete
        communication_start_time = datetime.now()
        MPI.Request.Waitall(pending_reqs)
        con.close()
        times['communication_time'] = (datetime.now() - communication_start_time).total_seconds()
        
    else:
        # Receive sizes first
        req_size_1 = comm.irecv(source=0, tag=200)
        req_size_2 = comm.irecv(source=0, tag=201)
        size_1 = req_size_1.wait()
        size_2 = req_size_2.wait()
        
        # Allocate buffers
        file_bytes_1 = bytearray(size_1)
        file_bytes_2 = bytearray(size_2)
        
        # Receive actual data
        req_data_1 = comm.Irecv([file_bytes_1, MPI.BYTE], source=0, tag=100)
        req_data_2 = comm.Irecv([file_bytes_2, MPI.BYTE], source=0, tag=101)
        req_data_1.Wait()
        req_data_2.Wait()
        
        # Write received data to local parquet files
        with open(local_parquet_path_1, 'wb') as f:
            f.write(file_bytes_1)
        with open(local_parquet_path_2, 'wb') as f:
            f.write(file_bytes_2)
    
    # Load partitioned data into local DuckDB
    if rank == 0:
        local_parquet_path_1 = f'/tmp/{table1_name}_part_{rank}.parquet'
        local_parquet_path_2 = f'/tmp/{table2_name}_part_{rank}.parquet'
    
    con = duckdb.connect(local_db_path)
    con.execute(f"DROP TABLE IF EXISTS {table1_name}")
    con.execute(f"DROP TABLE IF EXISTS {table2_name}")
    con.execute(f"CREATE TABLE {table1_name} AS SELECT * FROM read_parquet(?)", (local_parquet_path_1,))
    con.execute(f"CREATE TABLE {table2_name} AS SELECT * FROM read_parquet(?)", (local_parquet_path_2,))

    return con, times
