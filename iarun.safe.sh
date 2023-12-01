#!/usr/bin/env bash

systemd-run --scope -p MemoryMax=8G --user ./iarun.sh

