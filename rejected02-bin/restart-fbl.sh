#!/bin/bash
ps aux |grep '[f]eedbackloop_unsubscribe/production' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restarted feedbackloop unsubscribe rejected consumers'
/opt/rejected/bin/start-fbl.sh
