#!/bin/bash
ps aux |grep '[f]acebook_live_invite/etc' | awk '{print $2}' | sudo xargs kill
/opt/rejected/bin/start-fb-live-invite.sh
