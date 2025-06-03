from mpi4py import MPI
import duckdb
import os
import time
import sys
import mmap 
from datetime import datetime

def partition_and_distribute_parquet(comm, rank, size, table_1, table_2, join_key_1, join_key_2):
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_1 = f'/tmp/{table_1}_local.parquet'
    local_parquet_path_2 = f'/tmp/{table_2}_local.parquet'

    if rank == 0:
        con = duckdb.connect(db_path)
        for i in range(size):
            part_path_1 = f'/tmp/{table_1}_part_{i}.parquet'
            part_path_2 = f'/tmp/{table_2}_part_{i}.parquet'

            con.execute(f"""
                COPY (
                    SELECT * FROM {table_1} WHERE {join_key_1} % {size} = {i}
                ) TO '{part_path_1}' (FORMAT 'parquet')
            """)

            con.execute(f"""
                COPY (
                    SELECT * FROM {table_2} WHERE {join_key_2} % {size} = {i}
                ) TO '{part_path_2}' (FORMAT 'parquet')
            """)

            if i == 0:
                continue  # Coordinator doesn't need to send to itself

            # Send both Parquet files
            with open(part_path_1, 'rb') as f:
                comm.send(f.read(), dest=i, tag=100)
            with open(part_path_2, 'rb') as f:
                comm.send(f.read(), dest=i, tag=101)

            # print(f"Rank 0: Sent customer and orders partitions to rank {i}")

        con.close()

    else:
        # Worker nodes receive Parquet files
        # print(f"Rank {rank}: Receiving customer and orders Parquet files...")
        file_bytes_1 = comm.recv(source=0, tag=100)
        file_bytes_2 = comm.recv(source=0, tag=101)

        with open(local_parquet_path_1, 'wb') as f:
            f.write(file_bytes_2)
        with open(local_parquet_path_2, 'wb') as f:
            f.write(file_bytes_2)

        # print(f"Rank {rank}: Received and saved Parquet files")

    # Load into one local DuckDB file
    if rank == 0:
        # Coordinator reads local partitions
        local_parquet_path_1 = f'/tmp/{table_1}_part_{rank}.parquet'
        local_parquet_path_2 = f'/tmp/{table_2}_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)

    con.execute(f"CREATE TABLE IF NOT EXISTS {table_1} AS SELECT * FROM read_parquet(?)", (local_parquet_path_1,))
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_2} AS SELECT * FROM read_parquet(?)", (local_parquet_path_2,))

    # Verify counts
    count_1 = con.execute(f"SELECT COUNT(*) FROM {table_1}").fetchone()[0]
    count_2 = con.execute(f"SELECT COUNT(*) FROM {table_2}").fetchone()[0]
    print(f"Rank {rank}: Loaded {count_1} customer and {count_2} orders records into local DB")

    return con




def partition_and_distribute_customer_and_orders_parquet(comm, rank, size):
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

def partition_and_distribute_customer_and_orders_streaming_parquet(comm, rank, size):
    """
    Version 1: Asynchronous sends with pipelining
    Major improvements:
    - Non-blocking sends to overlap communication
    - Memory-mapped file reading for efficiency
    - Pipelined processing (partition while sending previous)
    """
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'
    
    if rank == 0:
        print("Rank 0: Starting optimized partitioning with async sends...")
        start_time = datetime.now()
        
        con = duckdb.connect(db_path)
        pending_sends = []
        
        for i in range(size):
            part_path_c = f'/tmp/customer_part_{i}.parquet'
            part_path_o = f'/tmp/orders_part_{i}.parquet'
            
            # Generate partitions
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
                continue
            
            # Use memory-mapped files for efficient reading
            with open(part_path_c, 'rb') as f:
                # with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm_c:
                #     file_bytes_c = mm_c.read()
                file_bytes_c = f.read()
                req_c = comm.isend(file_bytes_c, dest=i, tag=100)
            
            with open(part_path_o, 'rb') as f:
                # with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm_o:
                #     file_bytes_o = mm_o.read()
                file_bytes_o = f.read()
                req_o = comm.isend(file_bytes_o, dest=i, tag=101)
            
            # Non-blocking sends
            # req_c = comm.isend(file_bytes_c, dest=i, tag=100)
            # req_c.wait()
            # req_o = comm.isend(file_bytes_o, dest=i, tag=101)
            # req_o.wait()
            
            pending_sends.extend([req_c, req_o])
            
            # Clean up partition files immediately after reading
            os.remove(part_path_c)
            os.remove(part_path_o)
        
        # Wait for all sends to complete
        MPI.Request.waitall(pending_sends)
        con.close()
        
        elapsed = datetime.now() - start_time
        print(f"Rank 0: Partitioning and distribution completed in {elapsed}")
    
    else:
        # Non-blocking receives
        req_c = comm.irecv(source=0, tag=100)
        req_o = comm.irecv(source=0, tag=101)
        
        file_bytes_c = req_c.wait()
        file_bytes_o = req_o.wait()
        
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