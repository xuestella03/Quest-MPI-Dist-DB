- Biggest performance bottleneck is communication between nodes. Need to optimize:
    - Partioning time
    - Collection time (compare 4 implementations of this??)
- If code is not parallelized, one core on each node is better. 


## To-Do
- Try to track local join time (adding in each node and gather?)
- Clean up print statements
- Remove pip install
- Compare file transfer methods (parquet, csv, raw tuple data line by line)
- Use pandas to collect results

*Will remove older verisons of merge*