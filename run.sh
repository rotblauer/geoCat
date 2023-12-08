#!/bin/bash
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

path_to_tracks=${1:-/where/}
output_dir=${2:-/tmp/output}


# time cat "${path_to_tracks}"|zcat|python3 main.py --output_dir $output_dir
time cat "${path_to_tracks}"|zcat|python3 main.py --output_dir $output_dir --skip_existing
