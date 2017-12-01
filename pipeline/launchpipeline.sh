#!/bin/bash

export LD_LIBRARY_PATH=/usr/lib/jvm/java/jre/lib/amd64/server/:$LD_LIBRARY_PATH

cd /home/ec2-user/prom-rebuild/pipeline

if [ -z "$1" ]
  then
    echo "No purchase buckets file supplied"
    exit 1
fi

if [ -z "$2" ]
  then
    echo "No purchase data file supplied"
    exit 1
fi

PURCHASE_BUCKETS="$1"
PURCHASE_DATA="$2"

sed -i -e 's/<PURCHASE_BUCKETS>/'$PURCHASE_BUCKETS'/'\
       -e 's/<PURCHASE_DATA>/'$PURCHASE_DATA'/'\
    /home/ec2-user/prom-rebuild/pipeline/luigi.cfg

python -m luigi --local-scheduler --workers 1 --module pipeline PipelineTask