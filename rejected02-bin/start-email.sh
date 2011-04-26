#!/bin/bash
sudo su rejected -c "/opt/rejected/bin/rejected.py -c /opt/rejected/email_composer/etc/forgot_password.yaml -d"
sudo su rejected -c "/opt/rejected/bin/rejected.py -c /opt/rejected/email_composer/etc/match_iphone.yaml -d"
