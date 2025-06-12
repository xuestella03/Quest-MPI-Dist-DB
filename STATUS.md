- Biggest performance bottleneck is communication between nodes. Need to optimize:
    - Partioning time
    - Collection time (compare 4 implementations of this??)
- If code is not parallelized, one core on each node is better. 


## To-Do
- Try for other query as well
- Different scales

*Will remove older verisons of merge*


## Notes
run_join_1_4nodes.sh means run query 1 (orders and customers) on 4 nodes
run_batch_jobs_1_4nodes.sh means run the above 100 times