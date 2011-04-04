#!/bin/bash
ps aux |grep '[c]art/etc' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restarted cart rejected consumers'
/opt/rejected/bin/start-cart.sh
