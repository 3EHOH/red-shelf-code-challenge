import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from ...config import ModelConfig, MySQLDBConfig, PathConfig
from ...run_55 import Run55 
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
        command = ['mysql', '-f', '-h{}'.format(MySQLDBConfig().prd_host),
                   '--database={}'.format(DB),
                   '-u{}'.format(MySQLDBConfig().prd_user),
                   '-p{}'.format(MySQLDBConfig().prd_pass)]
        with open(SQL_FILE) as input_file:
            proc = subprocess.Popen(
                command, stdin = input_file, stderr=PIPE, stdout=PIPE)
            output,error = proc.communicate()
            with self.output().open('w') as out:
                out.write(str(error))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('providerattribution.py <JOBUID>')
        exit(-1)
    luigi.run([
        'ProviderAttribution', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
