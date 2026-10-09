#!/bin/bash
# Block until the given epoch second, then print the time. Usage: wait-until.sh <unix-seconds>
target=$1
while [ "$(date +%s)" -lt "$target" ]; do sleep 5; done
date -u +%FT%TZ
