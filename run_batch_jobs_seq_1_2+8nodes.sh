#!/bin/bash

# Submit 100 runs for 2-node jobs sequentially
jobid=$(sbatch run_join_1_2nodes.sh | awk '{print $4}')
echo "Submitted 2-node job 1 with ID $jobid"

for i in {2..100}; do
    jobid=$(sbatch --dependency=afterok:$jobid run_join_1_2nodes.sh | awk '{print $4}')
    echo "Submitted 2-node job $i with ID $jobid"
done

# Submit 100 runs for 8-node jobs sequentially AFTER 2-node jobs complete
jobid=$(sbatch --dependency=afterok:$jobid run_join_1_8nodes.sh | awk '{print $4}')
echo "Submitted 8-node job 1 with ID $jobid"

for i in {2..100}; do
    jobid=$(sbatch --dependency=afterok:$jobid run_join_1_8nodes.sh | awk '{print $4}')
    echo "Submitted 8-node job $i with ID $jobid"
done
