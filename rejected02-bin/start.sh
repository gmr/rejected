#!/bin/bash
for f in /opt/rejected/bin/start-*sh
do
  echo $f
  $f
done
sudo /opt/rejected/bin/setaffinity.sh 1> /dev/null
