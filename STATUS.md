- Biggest performance bottleneck is communication between nodes. Need to optimize:
    - Partioning time
    - Collection time (compare 4 implementations of this??)
- If code is not parallelized, one core on each node is better. 


## To-Do
- Try to track local join time (adding in each node and gather?)
- Clean up print statements
- Remove module loading and pip install