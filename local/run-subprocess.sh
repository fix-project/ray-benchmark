#!/usr/bin/env bash

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <program-path> <key-list> <num-keys> <max-n> <minio-port>"
  exit 1
fi

for ((i=1; i <=$5; i=i+100))
do
  for ((j=0; j<5; j++))
  do
    begin_index=$((j * $3))
    echo "python3 ../cluster/bptree-get-n-subprocess.py $1 $2 $begin_index $3 $4 $i"
    python3 ../cluster/bptree-get-n-subprocess.py $1 $2 $begin_index $3 $4 $i
  done
done
