#!/bin/bash
CORE=0
CORES=$(grep 'processor' /proc/cpuinfo -c)
PIDS=$(pgrep -f rejected.py)

echo "Setting CPU Affinity with $CORES cores to use."

for pid in $PIDS
do
  taskset -pc $CORE $pid
  CORE=$((${CORE} + 1))
  if [ $CORE -ge $CORES ]; then
    CORE=0
  fi
done
