import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from ....config import ModelConfig, MySQLDBConfig, PathConfig
from ....run_55 import Run55 
from hci3_reliability_analysis import HCI3ReliabilityAnalysis

STEP = 'ieva'

DB = ModelConfig().jobname + ModelConfig().rundate

SQL_FILE = os.path.join(PathConfig().postec_path,
                        'Master_IEVA_W_Description.sql')

class IEVA(luigi.Task):
    """ perform intra-episode variability analysis """
    datafile = luigi.Parameter(default=STEP)

    def requires(self):
        return [HCI3ReliabilityAnalysis()]

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
    luigi.run([
        'IEVA', 
        '--workers', '1',
        '--local-scheduler'])
