#!/bin/bash
#
if [ -z "$1" ]; then
    echo "Usage: $0 <command>"
    exit 1
fi

for i in {1..16}; do
    echo "Running command: $1 (Iteration $i)"
    eval $1
    sleep 5
done
