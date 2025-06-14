#!/bin/bash
module load mamba/24.3.0
module load mpi/openmpi-4.1.1

jobid=$(sbatch run_join_1_3nodes.sh | awk '{print $4}')
echo "Submitted 8-node job 1 with ID $jobid"

nodes=(1 3 8 16)

for i in {2..200}; do
    n=${nodes[$(( (i - 2) % 4 ))]}  # alternate between 1, 3, 8, 16
    script="run_join_1_${n}nodes.sh"

    jobid=$(sbatch --dependency=afterok:$jobid "$script" | awk '{print $4}')
    echo "Submitted ${n}-node job $i with ID $jobid"
done

