#!/bin/bash
#
# Usage: start.sh [ <consumer_name> [-v] ]
#
# If starting a single consumer, you can pass -v as the 2nd argument
# to run in the foreground

# By default, run in the background (-d = detached)
FGBG=${2:--d}

function get_count
{
  COUNT_FILE=${1//.yaml/.qty}
  COUNT=$(cat $COUNT_FILE)
  echo $COUNT
}

function start_consumers
{
  CONSUMER=$(basename ${1} .yaml)
  CONSUMERS=$(get_count $1)
  echo "Starting $CONSUMERS $CONSUMER consumer(s)"
  for x in $(seq 1 $CONSUMERS); do
    sudo su rejected -c "/opt/rejected/bin/rejected.py -c $1 ${FGBG}"
  done
  /opt/rejected/bin/opslog.py 'Started $CONSUMERS $CONSUMER rejected consumer(s)'
}

if [ -z "$1" ]; then
  for CONFIG in /opt/rejected/config/*yaml
  do
    start_consumers $CONFIG
  done
else
  for CONFIG in /opt/rejected/config/$1.yaml
  do
    start_consumers $CONFIG
  done
fi

sudo /opt/rejected/bin/setaffinity.sh 1> /dev/null
