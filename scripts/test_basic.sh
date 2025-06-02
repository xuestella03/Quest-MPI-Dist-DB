#!/bin/bash
#SBATCH --job-name=duckdb_basic_test
#SBATCH --account=e32695
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:10:00
#SBATCH --partition=short
#SBATCH --output=duckdb_basic_%j.out
#SBATCH --error=duckdb_basic_%j.err

echo "Starting DuckDB basic test at $(date)"
echo "Running on node: $(hostname)"

# Load required modules
# module load python/3.8.10
echo "Loaded modules"
echo "Python version: $(python3 --version)"

# Install DuckDB if not already installed
pip install --user duckdb

# Run the basic test
python3 src/test_duckdb_basic.py

echo "Basic test completed at $(date)"
