#!/bin/bash
ps aux |grep '[e]mail_spooler' | awk '{print $2}' | sudo xargs kill
ps aux |grep '[e]mail_composer' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restart email_spooler and email_composer rejected consumers'
/opt/rejected/bin/start-email.sh
