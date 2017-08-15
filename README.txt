You need Python 3, luigi, mysql-connector for this to run.

Get the next JOBUID via:

    python jobuid.py

Update luigi.cfg

Run with:

    python -m luigi --local-scheduler --workers 16 --module pipeline PipelineTask
