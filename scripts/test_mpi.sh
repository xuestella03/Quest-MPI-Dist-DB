#!/bin/bash
#SBATCH --job-name=duckdb_mpi_test
#SBATCH --account=e32695
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:10:00
#SBATCH --partition=short
#SBATCH --output=duckdb_mpi_%j.out
#SBATCH --error=duckdb_mpi_%j.err

echo "Starting DuckDB MPI test at $(date)"
echo "Running on $SLURM_NNODES nodes with $SLURM_NTASKS total tasks"

# Load required modules
#module load python/anaconda3
module load mamba/24.3.0
module load mpi/openmpi-4.1.1

# Install required packages if not already installed
pip install --user duckdb mpi4py
echo "Successfully installed packages"

# Run the MPI test
mpirun -np $SLURM_NTASKS python -u src/test_duckdb_mpi.py

echo "MPI test completed at $(date)"