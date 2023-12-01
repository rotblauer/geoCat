#!/usr/bin/env bash

set -x

time cat /home/ia/tdata/master.json.gz|zcat|python3 main.py --workers 1

# nice time cat /home/ia/tdata/master.json.gz|zcat|python3
# prlimit --as=16000000000 time cat /home/ia/tdata/master.json.gz|zcat|python3
# prlimit --as=4096000000 time cat /home/ia/tdata/master.json.gz|zcat|python3
# cpulimit -l 20 prlimit --as=4096000000 time cat /home/ia/tdata/master.json.gz|zcat|python3
# systemd-run --scope -p CPUQuota=20% -p MemoryMax=4096M -p MemoryHigh=3940M --user time cat /home/ia/tdata/master.json.gz|zcat|python3

# # time cat ~/tdata/last10k.txt|python3 main.py

# 10 million 1 work
# real	2m36.976s
# user	3m21.667s
# sys	0m12.339s


# 10 million 8 work

# real	2m34.773s
# user	3m20.102s
# sys	0m12.570s
