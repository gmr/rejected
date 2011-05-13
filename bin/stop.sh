#!/bin/bash

function stop_consumers
{
  CONSUMER=$(basename ${1} .yaml)
  echo "Sending TERM signal to $CONSUMER consumer(s)"
  sudo pkill -f $1
  /opt/rejected/bin/opslog.py "Stopped $CONSUMER rejected consumers"
}

if [ -z "$1" ]; then
  for CONFIG in /opt/rejected/config/*yaml
  do
    stop_consumers $CONFIG
  done
else
  for CONFIG in /opt/rejected/config/$1.yaml
  do
    stop_consumers $CONFIG
  done
fi

sudo /opt/rejected/bin/setaffinity.sh 1> /dev/null
