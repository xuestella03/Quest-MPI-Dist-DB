#!/usr/bin/env python3
"""
Test DuckDB with MPI across multiple Quest nodes
"""
from mpi4py import MPI
import duckdb
import os
import time
import random


def test_mpi_duckdb_basic(rank, size):
    """Test basic DuckDB operations on each MPI rank"""
    print(f"Rank {rank}: Testing DuckDB basic operations...")

    # Create unique database for each rank
    db_path = f"/tmp/test_rank_{rank}.duckdb"

    try:
        conn = duckdb.connect(db_path)

        # Create table with rank-specific data
        conn.execute("""
            CREATE TABLE rank_data (
                rank INTEGER,
                id INTEGER,
                hostname VARCHAR,
                random_value DECIMAL(10,2)
            )
        """)

        # Insert rank-specific data
        hostname = os.uname().nodename
        data = []
        for i in range(100):
            data.append((
                rank,
                i,
                hostname,
                random.uniform(1, 100)
            ))

        conn.executemany(
            "INSERT INTO rank_data VALUES (?, ?, ?, ?)",
            data
        )

        # Query local data
        count = conn.execute("SELECT COUNT(*) FROM rank_data").fetchone()[0]
        avg_val = conn.execute("SELECT AVG(random_value) FROM rank_data").fetchone()[0]

        conn.close()
        os.remove(db_path)

        return {
            'rank': rank,
            'hostname': hostname,
            'count': count,
            'avg_value': avg_val
        }

    except Exception as e:
        print(f"Rank {rank}: Error - {e}")
        if os.path.exists(db_path):
            os.remove(db_path)
        return None


def test_data_partitioning(rank, size):
    """Test simple data partitioning across ranks"""
    print(f"Rank {rank}: Testing data partitioning...")

    # Generate dataset where each rank gets specific records
    total_records = 1000
    records_per_rank = total_records // size
    start_id = rank * records_per_rank
    end_id = start_id + records_per_rank

    db_path = f"/tmp/partition_test_rank_{rank}.duckdb"

    try:
        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE TABLE partitioned_data (
                id INTEGER,
                rank_owner INTEGER,
                value DECIMAL(10,2),
                category VARCHAR
            )
        """)

        # Each rank inserts its partition
        data = []
        for i in range(start_id, end_id):
            data.append((
                i,
                rank,
                random.uniform(10, 500),
                f"Cat_{i % 5}"
            ))

        conn.executemany(
            "INSERT INTO partitioned_data VALUES (?, ?, ?, ?)",
            data
        )

        # Perform local aggregation
        local_results = conn.execute("""
            SELECT category,
                   COUNT(*) as count,
                   SUM(value) as total_value
            FROM partitioned_data
            GROUP BY category
            ORDER BY category
        """).fetchall()

        conn.close()
        os.remove(db_path)

        return local_results

    except Exception as e:
        print(f"Rank {rank}: Partitioning error - {e}")
        if os.path.exists(db_path):
            os.remove(db_path)
        return []


def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        print("=" * 60)
        print(f"DuckDB + MPI Test on {size} processes")
        print("=" * 60)

    # Test 1: Basic DuckDB on each rank
    local_result = test_mpi_duckdb_basic(rank, size)

    # Gather results from all ranks
    all_results = comm.gather(local_result, root=0)

    if rank == 0:
        print("\\nTest 1: Basic DuckDB Operations")
        print("-" * 40)
        for result in all_results:
            if result:
                print(f"Rank {result['rank']} ({result['hostname']}): "
                      f"{result['count']} records, avg={result['avg_value']:.2f}")
            else:
                print(f"Rank failed")

    # Synchronize before next test
    comm.Barrier()

    # Test 2: Data partitioning
    partition_results = test_data_partitioning(rank, size)

    # Gather partition results
    all_partition_results = comm.gather(partition_results, root=0)

    if rank == 0:
        print("\\nTest 2: Data Partitioning Results")
        print("-" * 40)

        # Combine results from all ranks
        combined_results = {}
        for rank_results in all_partition_results:
            for category, count, total_value in rank_results:
                if category not in combined_results:
                    combined_results[category] = {'count': 0, 'total': 0}
                combined_results[category]['count'] += count
                combined_results[category]['total'] += total_value

        print("Combined results across all ranks:")
        for category in sorted(combined_results.keys()):
            data = combined_results[category]
            avg = data['total'] / data['count'] if data['count'] > 0 else 0
            print(f"  {category}: {data['count']} records, "
                  f"total={data['total']:.2f}, avg={avg:.2f}")

        print(f"\\n✓ Successfully tested DuckDB across {size} MPI processes!")
        print("✓ Ready for distributed join implementation!")


if __name__ == "__main__":
    main()