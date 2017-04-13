#!/usr/bin/env bash

NOBATCH=1 ./fsl_sub -v -N local -l ./local -T 0 -q unlimited -a amd64 -p 0 -M foo@example.com -j 0 git log -n 1 --pretty=email
./fsl_sub -v -N helloworld -l ./helloworld -T 1 -q nimh echo hello world
FSL_QUEUE=nimh ./fsl_sub -v -N omp -l ./openmp -q quick -s openmp,4 echo "\$OMP_NUM_THREADS"
FSL_MEM=3 ./fsl_sub -v -N sleeptest -l ./sleeptest -T 1 -t sleeptest.sh
