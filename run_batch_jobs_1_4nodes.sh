#!/bin/bash

for i in {1..5}; do
    echo "Submitting run $i..."
    sbatch run_join_1_4nodes.sh
done