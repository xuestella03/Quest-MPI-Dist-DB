#!/bin/bash
#SBATCH --job-name=distributed_join_customer_orders
#SBATCH --account=e32695
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=6G
#SBATCH --time=00:20:00
#SBATCH --partition=short
#SBATCH --output=join_1_2nodes_%j.out
#SBATCH --error=join_1_2nodes_%j.err

echo "Starting distributed join for customer and orders tables"
echo "Job ID: $SLURM_JOB_ID"
echo "Running on $SLURM_NNODES nodes with $SLURM_NTASKS total tasks"

# load required modules
module load mamba/24.3.0
module load mpi/openmpi-4.1.1

# install duckdb and mpi4py if not already installed
# pip install --user duckdb mpi4py

# echo "Successfully loaded modules and installed packages"

# Run the tpch file upload test
# python3 -u src/generate_tpch.py
# echo "Finished uploading tpc-h file"

mpirun -np $SLURM_NTASKS python -u src/distributed_join_customer_orders.py
echo "job_id=$SLURM_JOB_ID"

echo "Distributed join for customer and orders tables finished"