#!/bin/bash
ps aux |grep '[i]rsupload/etc' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/start-irsupload.sh
