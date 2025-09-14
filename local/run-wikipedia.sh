#!/usr/bin/env bash

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <python-file> <style> <path-to-bptree-fix> <key-list> <num-keys> <tree-root-label>"
  exit 1
fi

for ((j=0; j<5; j++))
do
	begin_index=$((j * $5))
	python3 $1 $2 $3 $4 $begin_index $5 $6
	sleep 1
done
