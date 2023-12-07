#!/usr/bin/env bash

set -x
set +e

while ! ./dockerrun.sh; do echo "Dang! Retrying..."; done