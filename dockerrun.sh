#!/usr/bin/env bash

DOCKER_BUILD=0

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our own variables:
output_file=""
verbose=0

while getopts "h?b" opt; do
  case "$opt" in
    h|\?)
      show_help
      exit 0
      ;;
    b)  DOCKER_BUILD=1
      ;;
  esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

set -x

[[ $DOCKER_BUILD -eq 1 ]] && docker build -t geocat  .

docker run -i -t \
  --mount type=bind,source=$(pwd),target=/myworkdir \
  --mount type=bind,source=$HOME/tdata,target=/tdata \
  --mount type=bind,source=$(pwd)/output,target=/tmp/output \
  --memory 16GB \
  --memory-swap 64GB \
  geocat
