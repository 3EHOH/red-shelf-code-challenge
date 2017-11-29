#!/bin/bash

source /home/ec2-user/.bashrc

export LD_LIBRARY_PATH=/usr/lib/jvm/java/jre/lib/amd64/server/:$LD_LIBRARY_PATH
# adjust the number of workers depending on your count settings in luigi.cfg
# in NormanConfig and ConnieConfig

source ~/.bashrc

cd /home/ec2-user/prom-rebuild/luigi

if [ $# -gt 0 ]; then
    echo "Your command line contains $# arguments"
else
    echo "Your command line contains no arguments"
fi

CONFIG_FILE="$1"
RUN_ID="$2"

echo $CONFIG_FILE

python -m luigi --local-scheduler --workers 1 --module pipeline PipelineTask