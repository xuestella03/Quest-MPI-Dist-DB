from mpi4py import MPI
import duckdb
import os
import time
import sys
from datetime import datetime

def collect_results_streaming_parquet(comm, rank, size, conn, query, output_filename='joined_results.parquet'):
    """
    Stream Parquet files instead of raw data to collect distributed query results.
    This avoids Python object serialization overhead and works with any SQL query.
    
    Args:
        comm: MPI communicator
        rank: Current MPI rank
        size: Total number of MPI ranks
        conn: Local DuckDB connection
        query: SQL query to execute on each node (should be a SELECT statement)
        output_filename: Name of the final output parquet file (default: 'joined_results.parquet')
    
    Returns:
        tuple: (query_time, collection_time)
    """
    collection_time = 0
    # Each node writes local query results to a temporary Parquet file
    temp_parquet = f'/tmp/query_results_rank_{rank}.parquet'
   
    # Wrap the provided query in a COPY statement to export to Parquet
    copy_query = f"""
    COPY (
        {query.strip().rstrip(';')}
    ) TO '{temp_parquet}' (FORMAT 'parquet')
    """
   
    start_time = datetime.now()
    conn.execute(copy_query)
    query_time = datetime.now() - start_time
   
    # print(f"Rank {rank}: Local query completed in {query_time.total_seconds()}")
   
    if rank == 0:
        # print("Coordinator collecting Parquet files...")
        # start_time = datetime.now()
       
        # Final db for complete query results
        final_conn = duckdb.connect()
       
        # Read coordinator's own file first
        final_conn.execute(f"""
            CREATE TABLE query_result AS
            SELECT * FROM read_parquet('{temp_parquet}')
        """)
       
        start_time = datetime.now()
        # Collect Parquet files from other nodes
        for source_rank in range(1, size):
            # print(f"Receiving Parquet from rank {source_rank}...")
           
            # Receive file bytes
            file_bytes = comm.recv(source=source_rank, tag=300)
           
            # Write the bytes to a temporary parquet file
            remote_parquet = f'/tmp/query_results_from_rank_{source_rank}.parquet'
            with open(remote_parquet, 'wb') as f:
                f.write(file_bytes)
           
            # Append to main table using DuckDB parquet reader
            final_conn.execute(f"""
                INSERT INTO query_result
                SELECT * FROM read_parquet('{remote_parquet}')
            """)
           
            # Clean up temporary file
            os.remove(remote_parquet)
       
        # Export final result
        output_path = f'data/{output_filename}'
        final_conn.execute(f"""
            COPY query_result TO '{output_path}' (FORMAT 'parquet')
        """)
       
        collection_time = datetime.now() - start_time
        final_count = final_conn.execute("SELECT COUNT(*) FROM query_result").fetchone()[0]
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


