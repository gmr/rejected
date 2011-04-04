#!/bin/bash
ps aux |grep '[e]fferent/etc/production' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restarted efferent rejected consumers'
/opt/rejected/bin/start-efferent.sh
