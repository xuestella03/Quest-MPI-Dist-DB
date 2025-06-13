#!/bin/bash
module load mamba/24.3.0
module load mpi/openmpi-4.1.1



# Submit 100 runs for 2-node jobs sequentially
jobid=$(sbatch run_join_1_3nodes.sh | awk '{print $4}')
echo "Submitted 8-node job 1 with ID $jobid"

for i in {2..11}; do
    jobid=$(sbatch --dependency=afterok:$jobid run_join_1_3nodes.sh | awk '{print $4}')
    echo "Submitted 8-node job $i with ID $jobid"
done

# # Submit 100 runs for 8-node jobs sequentially AFTER 2-node jobs complete
# jobid=$(sbatch --dependency=afterok:$jobid run_join_1_3nodes.sh | awk '{print $4}')
# echo "Submitted 3-node job 1 with ID $jobid"

# for i in {2..50}; do
#     jobid=$(sbatch --dependency=afterok:$jobid run_join_1_3nodes.sh | awk '{print $4}')
#     echo "Submitted 3-node job $i with ID $jobid"
# done

# Submit 100 runs for 8-node jobs sequentially AFTER 2-node jobs complete
jobid=$(sbatch --dependency=afterok:$jobid run_join_1_1node.sh | awk '{print $4}')
echo "Submitted 1-node job 1 with ID $jobid"

for i in {2..50}; do
    jobid=$(sbatch --dependency=afterok:$jobid run_join_1_1node.sh | awk '{print $4}')
    echo "Submitted 1-node job $i with ID $jobid"
done
