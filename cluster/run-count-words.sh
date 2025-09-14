#!/bin/bash

./run-experiment.sh "python3 count-words.py good /mnt/fix/count-words/.fix chunk-100M A27" > 100m-good-output
./run-experiment.sh "python3 count-words.py bad /mnt/fix/count-words/.fix chunk-100M A27" > 100m-bad-output
