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

def partition_and_distribute_parquet(comm, rank, size, table_1, table_2, join_key_1, join_key_2):
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    # db_path = os.environ.get("DUCKDB_PATH", "data/coordinator/full_data/whole_tpch_0.1.duckdb")
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
            f.write(file_bytes_1)
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

def create_partition_worker(args):
    """
    Worker function to create a single partition in a separate process
    This allows parallel partition creation on multi-core coordinator
    """
    db_path, partition_id, size, output_dir = args
    
    # Each worker needs its own database connection
    con = duckdb.connect(db_path, read_only=True)
    
    part_path_c = f'{output_dir}/customer_part_{partition_id}.parquet'
    part_path_o = f'{output_dir}/orders_part_{partition_id}.parquet'
    
    try:
        # Create customer partition
        con.execute(f"""
            COPY (
                SELECT * FROM customer WHERE c_custkey % {size} = {partition_id}
            ) TO '{part_path_c}' (FORMAT 'parquet')
        """)
        
        # Create orders partition
        con.execute(f"""
            COPY (
                SELECT * FROM orders WHERE o_custkey % {size} = {partition_id}
            ) TO '{part_path_o}' (FORMAT 'parquet')
        """)
        
        # Get file sizes for progress tracking
        size_c = os.path.getsize(part_path_c) if os.path.exists(part_path_c) else 0
        size_o = os.path.getsize(part_path_o) if os.path.exists(part_path_o) else 0
        
        print(f"Worker: Created partition {partition_id} - Customer: {size_c} bytes, Orders: {size_o} bytes")
        
        return partition_id, part_path_c, part_path_o, size_c, size_o
        
    except Exception as e:
        print(f"Error creating partition {partition_id}: {e}")
        return partition_id, None, None, 0, 0
    finally:
        con.close()


def send_partition_worker(args):
    """
    Worker function to send partition files to MPI ranks
    This allows parallel sending while other partitions are being created
    """
    comm, rank, part_path_c, part_path_o = args
    
    try:
        # Read files
        with open(part_path_c, 'rb') as f:
            file_bytes_c = f.read()
        with open(part_path_o, 'rb') as f:
            file_bytes_o = f.read()
        
        # Send sizes first
        size_c = len(file_bytes_c)
        size_o = len(file_bytes_o)
        
        req_size_c = comm.isend(size_c, dest=rank, tag=200)
        req_size_o = comm.isend(size_o, dest=rank, tag=201)
        
        # Send actual data
        req_data_c = comm.Isend([file_bytes_c, MPI.BYTE], dest=rank, tag=100)
        req_data_o = comm.Isend([file_bytes_o, MPI.BYTE], dest=rank, tag=101)
        
        # Wait for completion
        req_size_c.wait()
        req_size_o.wait()
        req_data_c.Wait()
        req_data_o.Wait()
        
        # Clean up files after successful send
        os.remove(part_path_c)
        os.remove(part_path_o)
        
        print(f"Successfully sent partition to rank {rank}")
        return rank, True
        
    except Exception as e:
        print(f"Error sending to rank {rank}: {e}")
        return rank, False


def partition_and_distribute_1_multiprocessing(comm, rank, size):
    """
    Version 1: Use ProcessPoolExecutor for parallel partition creation
    Best for: Multi-core coordinators with fast local storage
    """
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    output_dir = '/tmp'
    
    if rank == 0:
        print("Rank 0: Starting multiprocessing partition creation...")
        start_time = datetime.now()
        
        # Determine number of worker processes (leave some cores for MPI communication)
        num_workers = max(1, mp.cpu_count() - 2)
        print(f"Using {num_workers} worker processes for partition creation")
        
        # Create partition arguments
        partition_args = [(db_path, i, size, output_dir) for i in range(size)]
        
        # Create partitions in parallel using processes
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            partition_futures = {
                executor.submit(create_partition_worker, args): args[1] 
                for args in partition_args
            }
            
            completed_partitions = {}
            
            # Process completed partitions as they finish
            for future in as_completed(partition_futures):
                partition_id = partition_futures[future]
                try:
                    result = future.result()
                    completed_partitions[result[0]] = result
                    print(f"Partition {partition_id} creation completed")
                except Exception as e:
                    print(f"Partition {partition_id} failed: {e}")
        
        partition_time = datetime.now() - start_time
        print(f"Partition creation completed in {partition_time}")
        
        # Send partitions to other ranks using threading for I/O overlap
        send_start = datetime.now()
        send_args = []
        
        for i in range(1, size):  # Skip rank 0 (coordinator)
            if i in completed_partitions:
                _, part_path_c, part_path_o, _, _ = completed_partitions[i]
                if part_path_c and part_path_o:
                    send_args.append((comm, i, part_path_c, part_path_o))
        
        # Use ThreadPoolExecutor for MPI communication (threads share MPI context)
        with ThreadPoolExecutor(max_workers=min(4, len(send_args))) as executor:
            send_futures = [
                executor.submit(send_partition_worker, args) 
                for args in send_args
            ]
            
            for future in as_completed(send_futures):
                try:
                    rank_id, success = future.result()
                    if success:
                        print(f"Successfully sent partition to rank {rank_id}")
                    else:
                        print(f"Failed to send partition to rank {rank_id}")
                except Exception as e:
                    print(f"Send operation failed: {e}")
        
        send_time = datetime.now() - send_start
        total_time = datetime.now() - start_time
        print(f"Send operations completed in {send_time}")
        print(f"Total partitioning and distribution completed in {total_time}")
    
    else:
        # Worker nodes receive partitions (same as original)
        local_parquet_path_c = '/tmp/customer_local.parquet'
        local_parquet_path_o = '/tmp/orders_local.parquet'
        
        print(f"Rank {rank}: Waiting for partition data...")
        
        # Receive sizes first
        req_size_c = comm.irecv(source=0, tag=200)
        req_size_o = comm.irecv(source=0, tag=201)
        size_c = req_size_c.wait()
        size_o = req_size_o.wait()
        
        print(f"Rank {rank}: Expecting {size_c} + {size_o} bytes")
        
        # Allocate buffers
        file_bytes_c = bytearray(size_c)
        file_bytes_o = bytearray(size_o)
        
        # Receive actual data
        req_data_c = comm.Irecv([file_bytes_c, MPI.BYTE], source=0, tag=100)
        req_data_o = comm.Irecv([file_bytes_o, MPI.BYTE], source=0, tag=101)
        req_data_c.Wait()
        req_data_o.Wait()
        
        # Write to local files
        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)
        
        print(f"Rank {rank}: Received and saved partition data")
    
    # Load into local DuckDB (same for all ranks)
    if rank == 0:
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'
    else:
        local_parquet_path_c = '/tmp/customer_local.parquet'
        local_parquet_path_o = '/tmp/orders_local.parquet'
    
    con = duckdb.connect(local_db_path)
    con.execute("DROP TABLE IF EXISTS customer")
    con.execute("DROP TABLE IF EXISTS orders")
    con.execute("CREATE TABLE customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))
    
    count_c = con.execute("SELECT COUNT(*) FROM customer").fetchone()[0]
    count_o = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    print(f"Rank {rank}: Loaded {count_c} customer and {count_o} orders records")
    
    return con