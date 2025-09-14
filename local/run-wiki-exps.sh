#!/usr/bin/env bash

bptree_large_roots=(16777216)
bptree_large_style=(bad good)

bptree_roots=(4096 256 64)
bptree_style=(bad good)

for r in "${bptree_roots[@]}"
do
	for s in "${bptree_style[@]}"
	do
		for ((i=0; i<17; i=i+1))
		do
			./run-wikipedia.sh bptree-get-string.py $s /mnt/minio/bptree-wikipedia/server/.fix/ /mnt/minio/bptree-wikipedia/client/wikipedia-samples 400 bptree-root-$r > ray-run-$r-$s-$i

		done
	done
done

for r in "${bptree_large_roots[@]}"
do
	for s in "${bptree_large_style[@]}"
	do
		for ((i=0; i<3; i=i+1))
		do
			./run-wikipedia.sh bptree-get-string-cache.py $s /mnt/minio/bptree-wikipedia/server/.fix/ /mnt/minio/bptree-wikipedia/client/wikipedia-samples 400 bptree-root-$r > ray-run-$r-$s-$i
		done
	done
done

