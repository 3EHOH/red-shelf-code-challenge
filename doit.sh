#!/bin/bash

# adjust the number of workers depending on your count settings in luigi.cfg
# in NormanConfig and ConnieConfig

python -m luigi --local-scheduler --workers 10 --module pipeline PipelineTask
