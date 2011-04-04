#!/bin/bash
ps aux |grep '[m]emoryhole/etc' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restarted memoryhole rejected consumers'
/opt/rejected/bin/start-memoryhole.sh
