#!/usr/bin/env bash

set -x

go build -o gocat .
time cat ~/tdata/master.json.gz | zcat | ./gocat --output ./go-output --batch-size 500000 --workers 6
# ...
# 2023/12/07 15:32:15 main.go:302: Wrote go-output/batch.441.size.500000_state_count.csv
# 2023/12/07 15:32:15 main.go:326: Wrote go-output/batch.441.size.500000_country_count.csv
# 2023/12/07 15:32:15 main.go:208: Wrote go-output/batch.442.size.397690_activity_count.csv
# 2023/12/07 15:32:16 main.go:302: Wrote go-output/batch.442.size.397690_state_count.csv
# 2023/12/07 15:32:16 main.go:326: Wrote go-output/batch.442.size.397690_country_count.csv
#
# real    16m3.717s
# user    131m28.411s
# sys     1m43.818s


mkdir -p ./go-output/go-output
time python3 main.py --skip_process_tracks --output ./go-output/ > /dev/null


# nice time cat /home/ia/tdata/master.json.gz|zcat|python3
# prlimit --as=16000000000 time cat /home/ia/tdata/master.json.gz|zcat|python3
# prlimit --as=4096000000 time cat /home/ia/tdata/master.json.gz|zcat|python3
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
