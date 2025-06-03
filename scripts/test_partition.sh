#!/bin/bash
#SBATCH --job-name=duckdb_partition_test
#SBATCH --account=e32695
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:10:00
#SBATCH --partition=short
#SBATCH --output=duckdb_partition_%j.out
#SBATCH --error=duckdb_partition_%j.err

echo "Starting DuckDB partition test at $(date)"
# echo "Running on node: $(hostname)"
echo "Running on $SLURM_NNODES nodes with $SLURM_NTASKS total tasks"

# Load required modules
# module load python/3.8.10
module load mamba/24.3.0
module load mpi/openmpi-4.1.1
echo "Loaded modules"
echo "Python version: $(python3 --version)"

# Install DuckDB if not already installed
pip install --user duckdb mpi4py

echo "Successfully installed packages"


# Run the tpch file upload test
python3 -u src/generate_tpch.py
# python3 -u src/test_duckdb_partition.py
mpirun -np $SLURM_NTASKS python -u src/test_duckdb_partition.py

echo "Partition test completed at $(date)"
