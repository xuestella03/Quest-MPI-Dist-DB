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

def partition_and_distribute_customer_and_orders_parquet(comm, rank, size):
    """
    Node 0 loads the TPC-H data, partitions the customer and orders tables based on c_custkey % size,
    and distributes Parquet partitions to all nodes.

    Each node receives their partitions and loads both into a single DuckDB file.

    Returns:
        duckdb.Connection: connection to the local DuckDB with customer and orders tables.
    """
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    # db_path = os.environ.get("DUCKDB_PATH", "data/coordinator/full_data/whole_tpch_0.1.duckdb")
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    if rank == 0:
        # Coordinator node
        # print("Rank 0: Partitioning and distributing customer and orders Parquet files")

        con = duckdb.connect(db_path, read_only=True)

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
        tuple: (local_connection, total_time)
    """
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_1 = f'/tmp/{table1_name}_local.parquet'
    local_parquet_path_2 = f'/tmp/{table2_name}_local.parquet'
    total_time = 0
    
    if rank == 0:
        print(f"Rank 0: Starting optimized partitioning for {table1_name} and {table2_name} with async sends...")
        start_time = datetime.now()
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
        
        # Wait for all async sends to complete
        MPI.Request.Waitall(pending_reqs)
        con.close()
        total_time = datetime.now() - start_time
        
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
    
    return con, total_time

def partition_and_distribute_customer_and_orders_streaming_parquet(comm, rank, size):
    db_path = 'data/coordinator/full_data/whole_tpch_5.duckdb'
    # db_path = os.environ.get("DUCKDB_PATH", "data/coordinator/full_data/whole_tpch_0.1.duckdb")
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    total_time = 0

    if rank == 0:
        print("Rank 0: Starting optimized partitioning with async sends...")
        start_time = datetime.now()

        con = duckdb.connect(db_path, read_only = True)
        pending_reqs = []

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

            with open(part_path_c, 'rb') as f:
                file_bytes_c = f.read()
            with open(part_path_o, 'rb') as f:
                file_bytes_o = f.read()

            if i != 0:
                # First, send sizes
                size_c = len(file_bytes_c)
                size_o = len(file_bytes_o)
                req_size_c = comm.isend(size_c, dest=i, tag=200)
                req_size_o = comm.isend(size_o, dest=i, tag=201)

                # Then send actual data
                req_data_c = comm.Isend([file_bytes_c, MPI.BYTE], dest=i, tag=100)
                req_data_o = comm.Isend([file_bytes_o, MPI.BYTE], dest=i, tag=101)

                pending_reqs.extend([req_size_c, req_size_o, req_data_c, req_data_o])

            # Clean up local partition files
            if i != 0:
                os.remove(part_path_c)
                os.remove(part_path_o)

        MPI.Request.Waitall(pending_reqs)
        con.close()

        total_time = datetime.now() - start_time

    else:
        # First receive sizes
        req_size_c = comm.irecv(source=0, tag=200)
        req_size_o = comm.irecv(source=0, tag=201)
        size_c = req_size_c.wait()
        size_o = req_size_o.wait()

        # Allocate buffers
        file_bytes_c = bytearray(size_c)
        file_bytes_o = bytearray(size_o)

        # Then receive actual data
        req_data_c = comm.Irecv([file_bytes_c, MPI.BYTE], source=0, tag=100)
        req_data_o = comm.Irecv([file_bytes_o, MPI.BYTE], source=0, tag=101)

        req_data_c.Wait()
        req_data_o.Wait()

        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)

    # Load into local DuckDB
    if rank == 0:
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)
    con.execute("DROP TABLE IF EXISTS customer")
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute("CREATE TABLE customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))

    return con, total_time


def partition_and_distribute_customer_and_orders_streaming_parquet_mmap(comm, rank, size):
    db_path = 'data/coordinator/full_data/whole_tpch_5.duckdb'
    # db_path = os.environ.get("DUCKDB_PATH", "data/coordinator/full_data/whole_tpch_0.1.duckdb")
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    if rank == 0:
        print("Rank 0: Starting optimized partitioning with async sends...")
        start_time = datetime.now()

        con = duckdb.connect(db_path)
        pending_reqs = []

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

            with open(part_path_c, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm_c:
                    file_bytes_c = mm_c.read()
            with open(part_path_o, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm_o:
                    file_bytes_o = mm_o.read()

            if i != 0:
                # First, send sizes
                size_c = len(file_bytes_c)
                size_o = len(file_bytes_o)
                req_size_c = comm.isend(size_c, dest=i, tag=200)
                req_size_o = comm.isend(size_o, dest=i, tag=201)

                # Then send actual data
                req_data_c = comm.Isend([file_bytes_c, MPI.BYTE], dest=i, tag=100)
                req_data_o = comm.Isend([file_bytes_o, MPI.BYTE], dest=i, tag=101)

                pending_reqs.extend([req_size_c, req_size_o, req_data_c, req_data_o])

            # Clean up local partition files
            if i != 0:
                os.remove(part_path_c)
                os.remove(part_path_o)

        MPI.Request.Waitall(pending_reqs)
        con.close()

        elapsed = datetime.now() - start_time
        print(f"Rank 0: Partitioning and distribution completed in {elapsed}")

    else:
        # First receive sizes
        req_size_c = comm.irecv(source=0, tag=200)
        req_size_o = comm.irecv(source=0, tag=201)
        size_c = req_size_c.wait()
        size_o = req_size_o.wait()

        # Allocate buffers
        file_bytes_c = bytearray(size_c)
        file_bytes_o = bytearray(size_o)

        # Then receive actual data
        req_data_c = comm.Irecv([file_bytes_c, MPI.BYTE], source=0, tag=100)
        req_data_o = comm.Irecv([file_bytes_o, MPI.BYTE], source=0, tag=101)

        req_data_c.Wait()
        req_data_o.Wait()

        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)

    # Load into local DuckDB
    if rank == 0:
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)
    con.execute("CREATE TABLE customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))

    return con

