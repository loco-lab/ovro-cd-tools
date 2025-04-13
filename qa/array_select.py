#! /bin/env python 
import sys
# array_select.py task_id task_count file1 file2 ... fileN
# N must be > task_count


task_id = sys.argv[1]
task_count = sys.argv[2]
files = sys.argv[3:]

assert len(files)>task_count, f'Error {len(files)} must be greater than $task_count' 

nfiles = len(files)
for i in range((task_id * nfiles)//task_count, ((task_id+1) * nfiles)//task_count):
    print(files[i])

