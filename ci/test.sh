#!/usr/bin/env sh
set -e
pip3 install -e '.[testing]'
mkdir -p build
pwd
flake8 --config=.flake8 --output build/flake8.txt --tee rejected tests
coverage run && coverage report && coverage xml
