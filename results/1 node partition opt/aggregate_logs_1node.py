import os
import glob
import re

# Directory containing your .out files
LOG_DIR = './'
log_files = glob.glob(os.path.join(LOG_DIR, 'join_1_1nodes_*.out'))

# Fields to collect
fields = ['total_time', 'partition_time', 'max_local_join_time', 'collection_time']

# Regex patterns
patterns = {
    'total_time': re.compile(r'total_time:\s*([\d.]+)\s*seconds'),
    'partition_time': re.compile(r'partition_time:\s*([\d.]+)\s*seconds'),
    'max_local_join_time': re.compile(r'max_local_join_time:\s*([\d.]+)\s*seconds'),
    'collection_time': re.compile(r'collection_time:\s*([\d.]+)\s*seconds'),
}

min_total_time = float('inf')
best_run_stats = {}
best_file = None

print(f"Found {len(log_files)} log files.")

for log_file in log_files:
    with open(log_file, 'r') as f:
        content = f.read()

    values = {}
    for key, pattern in patterns.items():
        match = pattern.search(content)
        if match:
            values[key] = float(match.group(1))
        else:
            print(f"Missing {key} in {log_file}")
            break

    if len(values) == len(fields):
        if values['total_time'] < min_total_time:
            min_total_time = values['total_time']
            best_run_stats = values
            best_file = log_file

if not best_run_stats:
    print("No valid logs found.")
else:
    print(f"\nBest run: {best_file}\nMinimum total_time: {min_total_time:.6f} seconds")
    print("Stats for best run:")
    for key in fields:
        print(f"  {key:20}: {best_run_stats[key]:.6f}")