import os
import sys
import luigi
import mysql.connector

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from epidedupe import Dedupe

STEP = 'providerattribution'

DB = ModelConfig().jobname + ModelConfig().rundate

SQL_FILE = os.path.join(PathConfig().postec_path, 'Provider_Attribution.sql')

class ProviderAttribution(luigi.Task):
    """ perform provider attribution """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [Dedupe(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):
        from subprocess import Popen, PIPE
        process = Popen(['mysql', '-h', MySQLDBConfig().prd_host,
                         '-D', DB, '-u', MySQLDBConfig().prd_user,
                         '-p', MySQLDBConfig().prd_pass],
                        stdout=PIPE, stdin=PIPE)
        ret = process.communicate(('SOURCE ' + SQL_FILE).encode())[0]
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('providerattribution.py <JOBUID>')
        exit(-1)
    luigi.run([
        'ProviderAttribution', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
