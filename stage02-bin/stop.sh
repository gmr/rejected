#!/bin/bash
ps aux | grep '[r]ejected.py' | awk '{print $2}' | sudo xargs kill
echo "SIGTERM sent"
