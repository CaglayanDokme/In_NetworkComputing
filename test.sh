#!/bin/bash

# Number of times to run the program
NUM_RUNS=10

# The program to run
PROGRAM="./build/In_NetworkComputing"

for ((ports=4; ports <= 32; ports+=2)); do
    for ((i=1; i<=NUM_RUNS; i++)); do
        ${PROGRAM} --log-filter=3 --ports=${ports} --network-computing=true;
    done
done
