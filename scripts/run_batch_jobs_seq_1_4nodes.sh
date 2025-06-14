#!/bin/bash
module load mamba/24.3.0
module load mpi/openmpi-4.1.1

# Submit the first job
jobid=$(sbatch run_join_1_16nodes.sh | awk '{print $4}')
echo "Submitted job 1 with ID $jobid"

# Submit the remaining 99 jobs, each depending on the one before it
for i in {2..15}; do
    jobid=$(sbatch --dependency=afterok:$jobid run_join_1_16nodes.sh | awk '{print $4}')
    echo "Submitted job $i with ID $jobid"
done
