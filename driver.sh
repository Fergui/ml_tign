#!/usr/bin/env bash
if [ $# -eq 0 ]
  then
     echo usage: ./driver.sh input.json
     exit 1
fi
cd $(dirname "$0")
export PYTHONPATH=src
python src/driver.py $1