import os
import glob
import re

# Directory containing your .out files
LOG_DIR = './'
log_files = glob.glob(os.path.join(LOG_DIR, 'join_1_8nodes_*.out'))

# Fields to collect
fields = [
    'total_time',
    'max_partition_phase_time',
    'partition_time',
    'coord_partition_time',
    'partition_communication_time',
    'max_local_join_time',
    'collection_time'
]

# Regex patterns for each field
patterns = {
    field: re.compile(rf'{field}:\s*([\d.]+)\s*seconds')
    for field in fields
}

# Initialize accumulators
sums = {field: 0.0 for field in fields}
counts = 0
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
        # Update sums for averaging
        for key in fields:
            sums[key] += values[key]
        counts += 1

        # Track minimum total_time
        if values['total_time'] < min_total_time:
            min_total_time = values['total_time']
            best_run_stats = values.copy()
            best_file = log_file

if counts == 0:
    print("No valid logs found.")
else:
    print(f"\nProcessed {counts} valid log files.")

    print("\nAverage Times (in seconds):")
    for key in fields:
        avg = sums[key] / counts
        print(f"  {key:30}: {avg:.6f}")

    print(f"\nBest run: {best_file}\nMinimum total_time: {min_total_time:.6f} seconds")
    print("Stats for best run:")
    for key in fields:
        print(f"  {key:30}: {best_run_stats[key]:.6f}")
