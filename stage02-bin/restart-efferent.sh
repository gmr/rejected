#!/bin/bash
ps aux |grep '[e]fferent/etc' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/start-efferent.sh
