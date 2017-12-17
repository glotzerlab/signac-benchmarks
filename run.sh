#!/bin/bash

for tool in signac datreant.core; do
  for N in 100 1000 10000; do
    python run_benchmark.py $tool -N $N $@
  done
done
