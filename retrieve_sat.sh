#!/usr/bin/env bash
if [ $# -eq 0 ]
  then
     echo usage: ./retrieve_sat.sh input.json
     exit 1
fi
cd $(dirname "$0")
export PYTHONPATH=src
python src/ingest/retrieve_sat.py $1
