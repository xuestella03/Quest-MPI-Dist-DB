# import os
# import glob
# import re

# # Directory containing your .out files
# LOG_DIR = './'
# log_files = glob.glob(os.path.join(LOG_DIR, '*.out'))

# # Fields to collect
# fields = ['total_time', 'partition_time', 'max_local_join_time', 'collection_time']
# sums = {field: 0.0 for field in fields}
# counts = 0

# # Regex patterns
# patterns = {
#     'total_time': re.compile(r'total_time:\s*([\d.]+)\s*seconds'),
#     'partition_time': re.compile(r'partition_time:\s*([\d.]+)\s*seconds'),
#     'max_local_join_time': re.compile(r'max_local_join_time:\s*([\d.]+)\s*seconds'),
#     'collection_time': re.compile(r'collection_time:\s*([\d.]+)\s*seconds'),
# }

# print(f"Found {len(log_files)} log files.")

# for log_file in log_files:
#     with open(log_file, 'r') as f:
#         content = f.read()

#     values = {}
#     for key, pattern in patterns.items():
#         match = pattern.search(content)
#         if match:
#             values[key] = float(match.group(1))
#         else:
#             print(f"Missing {key} in {log_file}")
#             break

#     if len(values) == len(fields):
#         for key in fields:
#             sums[key] += values[key]
#         counts += 1

# if counts == 0:
#     print("No valid logs found.")
# else:
#     print(f"\nProcessed {counts} valid runs.\nAverage Times (in seconds):")
#     for key in fields:
#         avg = sums[key] / counts
#         print(f"  {key:20}: {avg:.6f}")

import os
import glob
import re
import math

# Directory containing your .out files
LOG_DIR = './'
log_files = glob.glob(os.path.join(LOG_DIR, '*.out'))

# Fields to collect
fields = ['total_time', 'partition_time', 'max_local_join_time', 'collection_time']
sums = {field: 0.0 for field in fields}
counts = 0

# For variance computation (Welford's method)
means = {field: 0.0 for field in fields}
M2 = {field: 0.0 for field in fields}

# Regex patterns
patterns = {
    'total_time': re.compile(r'total_time:\s*([\d.]+)\s*seconds'),
    'partition_time': re.compile(r'partition_time:\s*([\d.]+)\s*seconds'),
    'max_local_join_time': re.compile(r'max_local_join_time:\s*([\d.]+)\s*seconds'),
    'collection_time': re.compile(r'collection_time:\s*([\d.]+)\s*seconds'),
}

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
        counts += 1
        for key in fields:
            x = values[key]
            delta = x - means[key]
            means[key] += delta / counts
            delta2 = x - means[key]
            M2[key] += delta * delta2
            sums[key] += x

if counts == 0:
    print("No valid logs found.")
else:
    print(f"\nProcessed {counts} valid runs.\nAverage Times and Standard Deviations (in seconds):")
    for key in fields:
        avg = means[key]
        stddev = math.sqrt(M2[key] / counts) if counts > 1 else 0.0
        print(f"  {key:20}: mean = {avg:.6f}, stddev = {stddev:.6f}")
