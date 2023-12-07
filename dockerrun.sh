#!/usr/bin/env bash

set -x

#docker build -t geocat  .

mkdir -p $(pwd)/output

docker run -i -t \
  --mount type=bind,source=$HOME/tdata,target=/tdata \
  --mount type=bind,source=$(pwd)/output,target=/tmp/output \
  --mount type=bind,source=$(pwd),target=/app \
  --memory 16GB \
  --memory-swap 32GB \
  geocat
