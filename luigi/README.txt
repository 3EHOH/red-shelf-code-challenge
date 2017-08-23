You need Python 3, luigi, and MySQL Connector/Python for this to run.

Get the next JOBUID via:

    python jobuid.py

Update the jobuid near the top of luigi.cfg (and whatever else needs updating...)

Make sure that libjvm.so is in your LD_LIBRARY_PATH. Typically, libjvm.so is in
$JAVA_HOME/lib/amd64/server

Kick-off the workflow via:

    python -m luigi --local-scheduler --workers 16 --module pipeline PipelineTask

On a r3.8xlarge EC2 instance with local MySQL DB and MongoDB the entire process
takes about 6 hours.
