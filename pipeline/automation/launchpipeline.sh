#!/bin/bash

#cd /home/ec2-user/prom-rebuild/pipeline

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

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

sed -i -e 's/.*purchase_buckets.*/'purchase_buckets=$PURCHASE_BUCKETS'/'\
       -e 's/.*purchase_data.*/'purchase_data=$PURCHASE_DATA'/'\
    luigi.cfg


python -m luigi --local-scheduler --workers 1 --module pipeline PipelineTask