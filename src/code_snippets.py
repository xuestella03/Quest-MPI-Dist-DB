
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


        