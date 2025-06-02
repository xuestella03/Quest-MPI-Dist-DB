#!/usr/bin/env python3
"""
Basic DuckDB functionality test on a single Quest node
"""
import duckdb
import os
import time
import sys
from datetime import datetime


def test_duckdb_installation():
    """Test if DuckDB is properly installed"""
    print("Testing DuckDB installation...")
    try:
        import duckdb
        print(f"✓ DuckDB version: {duckdb.__version__}")
        return True
    except ImportError as e:
        print(f"✗ DuckDB import failed: {e}")
        return False


def test_basic_operations():
    """Test basic DuckDB operations"""
    print("\nTesting basic DuckDB operations...")

    # Create in-memory database
    conn = duckdb.connect(':memory:')

    try:
        # Test table creation
        conn.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                value DECIMAL(10,2)
            )
        """)
        print("✓ Table creation successful")

        # Test data insertion
        test_data = [
            (1, 'Alice', 100.50),
            (2, 'Bob', 200.75),
            (3, 'Charlie', 150.25)
        ]

        conn.executemany(
            "INSERT INTO test_table VALUES (?, ?, ?)",
            test_data
        )
        print("✓ Data insertion successful")

        # Test query execution
        results = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        print(f"✓ Query execution successful - {len(results)} rows returned")

        # Test aggregation
        total = conn.execute("SELECT SUM(value) FROM test_table").fetchone()[0]
        print(f"✓ Aggregation successful - Total value: {total}")

        conn.close()
        return True

    except Exception as e:
        print(f"✗ Basic operations failed: {e}")
        conn.close()
        return False


def test_file_database():
    """Test file-based database operations"""
    print("\nTesting file-based database...")

    db_path = "/tmp/test_duckdb.db"

    try:
        # Clean up any existing database
        if os.path.exists(db_path):
            os.remove(db_path)

        # Create file database
        conn = duckdb.connect(db_path)

        # Create and populate table
        conn.execute("""
            CREATE TABLE performance_test (
                id INTEGER,
                timestamp TIMESTAMP,
                category VARCHAR,
                amount DECIMAL(12,2)
            )
        """)

        # Insert larger dataset
        import random
        from datetime import datetime, timedelta

        start_time = time.time()
        data = []
        base_date = datetime(2024, 1, 1)

        for i in range(10000):
            data.append((
                i,
                base_date + timedelta(days=random.randint(0, 365)),
                f"Category_{random.randint(1, 10)}",
                random.uniform(10, 1000)
            ))

        conn.executemany(
            "INSERT INTO performance_test VALUES (?, ?, ?, ?)",
            data
        )

        insert_time = time.time() - start_time
        print(f"✓ Inserted 10,000 rows in {insert_time:.2f} seconds")

        # Test complex query
        start_time = time.time()
        results = conn.execute("""
            SELECT category, 
                   COUNT(*) as count,
                   AVG(amount) as avg_amount,
                   SUM(amount) as total_amount
            FROM performance_test
            GROUP BY category
            ORDER BY total_amount DESC
        """).fetchall()

        query_time = time.time() - start_time
        print(f"✓ Complex query completed in {query_time:.3f} seconds")
        print(f"  Found {len(results)} categories")

        conn.close()

        # Verify file was created
        if os.path.exists(db_path):
            file_size = os.path.getsize(db_path) / (1024 * 1024)  # MB
            print(f"✓ Database file created: {file_size:.2f} MB")
            os.remove(db_path)  # Cleanup

        return True

    except Exception as e:
        print(f"✗ File database test failed: {e}")
        if os.path.exists(db_path):
            os.remove(db_path)
        return False


def test_csv_operations():
    """Test CSV reading capabilities"""
    print("\nTesting CSV operations...")

    try:
        # Create test CSV
        csv_path = "/tmp/test_data.csv"
        with open(csv_path, 'w') as f:
            f.write("id,name,score\n")
            for i in range(1000):
                f.write(f"{i},User_{i},{random.randint(50, 100)}\n")

        conn = duckdb.connect(':memory:')

        # Test direct CSV reading
        start_time = time.time()
        results = conn.execute(f"SELECT COUNT(*) FROM '{csv_path}'").fetchone()
        read_time = time.time() - start_time

        print(f"✓ CSV reading successful - {results[0]} rows in {read_time:.3f}s")

        # Test CSV with SQL operations
        avg_score = conn.execute(f"""
            SELECT AVG(score) 
            FROM '{csv_path}'
            WHERE score > 75
        """).fetchone()[0]

        print(f"✓ CSV SQL operations successful - Avg high score: {avg_score:.2f}")

        conn.close()
        os.remove(csv_path)
        return True

    except Exception as e:
        print(f"✗ CSV operations failed: {e}")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        return False


def main():
    start = datetime.now()
    print(f"Starting time: {start}")
    print("=" * 60)
    print("DuckDB Basic Functionality Test on Quest")
    print("=" * 60)

    # Get node information
    hostname = os.uname().nodename
    print(f"Running on node: {hostname}")
    print(f"Python version: {sys.version}")

    # Run all tests
    tests = [
        ("DuckDB Installation", test_duckdb_installation),
        ("Basic Operations", test_basic_operations),
        ("File Database", test_file_database),
        ("CSV Operations", test_csv_operations)
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"✗ {test_name} crashed: {e}")
            results[test_name] = False

    # Summary
    print("\\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = 0
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"{test_name:.<40} {status}")
        if result:
            passed += 1

    print(f"\\nOverall: {passed}/{len(tests)} tests passed")

    end = datetime.now()

    if passed == len(tests):
        print("✓ All tests passed! DuckDB is ready for distributed operations.")
        
        print(f"Ending time: {end}")
        print(f"Time elapsed: {end - start}")
        return 0
    else:
        print("✗ Some tests failed. Check the output above.")
        print(f"Ending time: {end}")
        print(f"Time elapsed: {end - start}")
        return 1


if __name__ == "__main__":
    import random

    exit(main())
