#!/usr/bin/env bash

set -x
docker build -t geocat  .
docker run -i -t \
  --mount type=bind,source=$HOME/tdata,target=/tdata \
  --mount type=bind,source=$(pwd)/output,target=/tmp/output \
  --memory 16GB \
  --memory-swap 32GB \
  geocat
