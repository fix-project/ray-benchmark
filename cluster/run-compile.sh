#!/bin/bash

./run-experiment.sh "python3 compile-subprocess.py /mnt/minio/ow-actions/build/src/driver/ wasm-files link-elfs-fix-wasm lld-out 32338 1977 -d" > lld-on-demand-output 
