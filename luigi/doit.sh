#!/bin/bash

export LD_LIBRARY_PATH=/usr/lib/jvm/java/jre/lib/amd64/server/:$LD_LIBRARY_PATH

# adjust the number of workers depending on your count settings in luigi.cfg
# in NormanConfig and ConnieConfig

cd /home/ec2-user/payformance/luigi

python -m luigi --local-scheduler --workers 10 --module pipeline PipelineTask
