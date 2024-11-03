#!/bin/bash

# The program to run
PROGRAM="./build/In_NetworkComputing"

for ((ports=4; ports <= 48; ports+=2)); do
    echo "[INFO] Running with ${ports} ports..";

    ${PROGRAM} --log-filter=3 --ports=${ports} --network-computing=true || exit 1;
done

echo "[INFO] Done";
exit 0;
