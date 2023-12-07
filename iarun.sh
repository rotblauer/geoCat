#!/usr/bin/env bash

set -x

go build -o gocat .
time cat ~/tdata/master.json.gz | zcat | ./gocat --output ./go-output --batch-size 500000 --workers 3
# ...
# 2023/12/07 14:06:58 main.go:404: Batch 439 GOROUTINES 9 2023-11-14T17:23:48Z
# 2023/12/07 14:06:59 main.go:313: Wrote go-output/batch.429.size.500000_state_count.csv
# 2023/12/07 14:06:59 main.go:337: Wrote go-output/batch.429.size.500000_country_count.csv
# 2023/12/07 14:07:00 main.go:223: Wrote go-output/batch.430.size.500000_activity_count.csv
# 2023/12/07 14:07:01 main.go:313: Wrote go-output/batch.430.size.500000_state_count.csv
# 2023/12/07 14:07:01 main.go:337: Wrote go-output/batch.430.size.500000_country_count.csv
#
# real    16m52.980s
# user    122m18.348s
# sys     1m47.588s

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
