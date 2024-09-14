#!/usr/bin/env bash
# Usage:./run.sh style path-to-bptree-fix key-to-look-up max-n

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <style> <path-to-bptree-fix> <key-list> <num-keys> <max-n>"
  exit 1
fi

for ((i=1; i <=$5; i=i+100))
do
  for ((j=0; j<5; j++))
  do
    begin_index=$((j * $4))
    end_index=$((begin_index + $4))
    echo "python3 bptree-get-n-ray.py $1 $2 $3 $begin_index $4 $i"
    python3 bptree-get-n-ray.py $1 $2 $3 $begin_index $4 $i
  done
done
