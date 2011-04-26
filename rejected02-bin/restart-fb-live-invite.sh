#!/bin/bash
ps aux |grep '[f]acebook_live_invite/etc/production' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/opslog.py 'Restarted facebook_live_invite unsubscribe rejected consumers'
/opt/rejected/bin/start-fb-live-invite.sh
