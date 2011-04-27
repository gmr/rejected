#!/bin/bash
for f in /opt/rejected/bin/restart-*sh
do
  echo $f
  $f
done
sudo /opt/rejected/bin/setaffinity.sh 1> /dev/null
