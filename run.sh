#!/bin/bash
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

path_to_tracks=${1:-/Volumes/SandCat/tdata/master.json.gz}

time cat "${path_to_tracks}"|zcat|python3 main.py --output_dir /tmp/output
time cat "${path_to_tracks}"|zcat|python3 main.py --output_dir /tmp/output --skip


# # time cat ~/tdata/last10k.txt|python3 main.py

# 10 million 1 work
# real	2m36.976s
# user	3m21.667s
# sys	0m12.339s


# 10 million 8 work

# real	2m34.773s
# user	3m20.102s
# sys	0m12.570s