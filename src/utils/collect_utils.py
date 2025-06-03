from mpi4py import MPI
import duckdb
import os
import time
import sys
from datetime import datetime

def collect_results_parquet_streaming_1(comm, rank, size, conn):
    """
    Stream Parquet files instead of raw data
    This avoids Python object serialization overhead
    For query 1 (customer and orders)
    """

    collection_time = 0
    # each node writes local join results to a temporary Parquet file
    temp_parquet = f'/tmp/join_results_rank_{rank}.parquet'
    
    query = f"""
    COPY (
        SELECT
            c.c_custkey,
            c.c_name,
            o.o_orderkey,
            o.o_orderdate,
            o.o_totalprice
        FROM customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
    ) TO '{temp_parquet}' (FORMAT 'parquet')
    """
    
    start_time = datetime.now()
    conn.execute(query)
    query_time = datetime.now() - start_time
    
    # print(f"Rank {rank}: Local join query completed in {query_time.total_seconds()}")
    
    if rank == 0:
        # print("Coordinator collecting Parquet files...")
        start_time = datetime.now()
        
        # final db for complete join results
        final_conn = duckdb.connect()
        
        # read coordinator's own file first
        final_conn.execute(f"""
            CREATE TABLE join_result AS 
            SELECT * FROM read_parquet('{temp_parquet}' )
        """)
        
        # Collect Parquet files from other nodes
        for source_rank in range(1, size):
            # print(f"Receiving Parquet from rank {source_rank}...")
            
            # receive file bytes
            file_bytes = comm.recv(source=source_rank, tag=300)
            
            # write the bytes to a temporary parquet file
            remote_parquet = f'/tmp/join_results_from_rank_{source_rank}.parquet'
            with open(remote_parquet, 'wb') as f:
                f.write(file_bytes)
            
            # append to main table using duckdb parquet
            final_conn.execute(f"""
                INSERT INTO join_result 
                SELECT * FROM read_parquet('{remote_parquet}')
            """)
            
            # clean up temporary file
            os.remove(remote_parquet)
        
        # export final result
        output_path = 'data/joined_results.parquet'
        final_conn.execute(f"""
            COPY join_result TO '{output_path}' (FORMAT 'parquet')
        """)
        
        collection_time = datetime.now() - start_time
        final_count = final_conn.execute("SELECT COUNT(*) FROM join_result").fetchone()[0]
        # print(f"Collection completed in {collection_time}, total records: {final_count}")
        
        final_conn.close()
        
    else:
        # Send Parquet file to coordinator
        with open(temp_parquet, 'rb') as f:
            file_bytes = f.read()
        comm.send(file_bytes, dest=0, tag=300)
    
    # Clean up local temporary file
    if os.path.exists(temp_parquet):
        os.remove(temp_parquet)

    return query_time, collection_time