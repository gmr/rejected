#!/bin/bash
for f in /opt/rejected/bin/restart-*sh
do
  exec $f
done
/opt/rejected/bin/setaffinity.sh
