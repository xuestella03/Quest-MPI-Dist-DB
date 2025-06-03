
###### Running

###### Initial method


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

    # perform local join
    local_join_start_time = datetime.now()
    local_results = perform_local_join(rank, conn)
    local_join_time = datetime.now() - local_join_start_time

    # collect results
    collection_start_time = datetime.now()
    collect_results(comm, rank, size, local_results)
    collection_time = datetime.now() - collection_start_time

    # cleanup
    cleanup(rank)

    total_time = datetime.now() - total_start_time 
    if rank == 0:
        print(f"total_time: {total_time.total_seconds()} seconds")
        print(f"partition_time: {partition_time.total_seconds()} seconds")
        print(f"local_join_time: {local_join_time.total_seconds()} seconds")
        print(f"collection_time: {collection_time.total_seconds()} seconds")


###### For parquet streaming

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
    conn = partition_utils.partition_and_distribute_customer_and_orders_streaming_parquet(comm, rank, size)

    # synchronize all processes after partitioning and distributing
    comm.Barrier()
    partition_time = datetime.now() - partition_start_time 

    # collect results
    local_join_time, collection_time = collect_utils.collect_results_parquet_streaming_1(comm, rank, size, conn)
    
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



###### Collection
def collect_results(comm, rank, size, local_results):

    all_results = comm.gather(local_results, root=0)

    if rank == 0:

        merged_results = []
        for result in all_results:
            merged_results.extend(result)

         # create DuckDB connection
        con = duckdb.connect()

        # create a table from the result list
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

        # write to Parquet file
        output_path = 'data/joined_results.parquet'
        con.execute(f"""
            COPY join_result TO '{output_path}' (FORMAT 'parquet')
        """)

        con.close()

def collect_results_parquet_streaming_1(comm, rank, size, conn):

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
    local_join_time = datetime.now() - start_time
    
    if rank != 0:
        # send parquet file to coordinator with MPI send
        with open(temp_parquet, 'rb') as f:
            file_bytes = f.read()
        comm.send(file_bytes, dest=0, tag=300)

    else:
        start_time = datetime.now()
        
        # final db for complete join results
        final_conn = duckdb.connect()
        
        # read coordinator's own local join file first
        final_conn.execute(f"""
            CREATE TABLE join_result AS 
            SELECT * FROM read_parquet('{temp_parquet}' )
        """)
        
        # collect parquet files from other nodes using recv for each source rank
        for source_rank in range(1, size):
            
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
        
        final_conn.close()
        
    # clean up
    if os.path.exists(temp_parquet):
        os.remove(temp_parquet)

    return local_join_time, collection_time


################# Partitioning

def partition_and_distribute_customer_and_orders_parquet(comm, rank, size):
    
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    if rank == 0:

        con = duckdb.connect(db_path, read_only=True)
        for i in range(size):
            part_path_c = f'/tmp/customer_part_{i}.parquet'
            part_path_o = f'/tmp/orders_part_{i}.parquet'

            con.execute(f"""COPY (SELECT * FROM customer WHERE c_custkey % {size} = {i}) 
                            TO '{part_path_c}' (FORMAT 'parquet')""")
            con.execute(f"""COPY (SELECT * FROM orders WHERE o_custkey % {size} = {i}) 
                            TO '{part_path_o}' (FORMAT 'parquet')""")
 
            if i != 0: # coordinator doesn't need to send to itself
                # send both Parquet files
                with open(part_path_c, 'rb') as f:
                    comm.send(f.read(), dest=i, tag=100)
                with open(part_path_o, 'rb') as f:
                    comm.send(f.read(), dest=i, tag=101)
        con.close()

    else:
        file_bytes_c = comm.recv(source=0, tag=100)
        file_bytes_o = comm.recv(source=0, tag=101)

        with open(local_parquet_path_c, 'wb') as f:
            f.write(file_bytes_c)
        with open(local_parquet_path_o, 'wb') as f:
            f.write(file_bytes_o)

    if rank == 0:
        # coordinator reads its own local partitions (you don't send and recv to yourself)
        local_parquet_path_c = f'/tmp/customer_part_{rank}.parquet'
        local_parquet_path_o = f'/tmp/orders_part_{rank}.parquet'

    con = duckdb.connect(local_db_path)
    con.execute("CREATE TABLE IF NOT EXISTS customer AS SELECT * FROM read_parquet(?)", (local_parquet_path_c,))
    con.execute("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM read_parquet(?)", (local_parquet_path_o,))
    
    return con


def partition_and_distribute_customer_and_orders_streaming_parquet(comm, rank, size):
    db_path = 'data/coordinator/full_data/whole_tpch_0.1.duckdb'
    local_db_path = f'/tmp/node_{rank}.duckdb'
    local_parquet_path_c = '/tmp/customer_local.parquet'
    local_parquet_path_o = '/tmp/orders_local.parquet'

    if rank == 0:
        con = duckdb.connect(db_path, read_only=True)
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
                # issue we had "MPI_ERR_TRUNCATED" (means not enough space in buffer)
                # to resolve: manually allocate recvbuffer size using Isend instead of isend
                # (isend sends a Python object, Isend allows for lower level)
                # send sizes
                size_c = len(file_bytes_c)
                size_o = len(file_bytes_o)
                req_size_c = comm.isend(size_c, dest=i, tag=200)
                req_size_o = comm.isend(size_o, dest=i, tag=201)

                # then send actual data
                req_data_c = comm.Isend([file_bytes_c, MPI.BYTE], dest=i, tag=100)
                req_data_o = comm.Isend([file_bytes_o, MPI.BYTE], dest=i, tag=101)

                pending_reqs.extend([req_size_c, req_size_o, req_data_c, req_data_o])

            # clean up local partition files
            if i != 0:
                os.remove(part_path_c)
                os.remove(part_path_o)

        MPI.Request.Waitall(pending_reqs)
        con.close()

    else:
        # receive sizes
        req_size_c = comm.irecv(source=0, tag=200)
        req_size_o = comm.irecv(source=0, tag=201)
        size_c = req_size_c.wait()
        size_o = req_size_o.wait()

        # alloc buffers
        file_bytes_c = bytearray(size_c)
        file_bytes_o = bytearray(size_o)

        # receive actual data
        req_data_c = comm.Irecv([file_bytes_c, MPI.BYTE], source=0, tag=100)
        req_data_o = comm.Irecv([file_bytes_o, MPI.BYTE], source=0, tag=101)

        # must wait until message is completed
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

    return con