#!/bin/bash

source /home/ec2-user/.bashrc

export LD_LIBRARY_PATH=/usr/lib/jvm/java/jre/lib/amd64/server/:$LD_LIBRARY_PATH
export SLACK_API_TOKEN=<your_generated_toke>
# adjust the number of workers depending on your count settings in luigi.cfg
# in NormanConfig and ConnieConfig

source ~/.bashrc

cd /home/ec2-user/payformance/luigi

python -m luigi --local-scheduler --workers 10 --module pipeline PipelineTask
