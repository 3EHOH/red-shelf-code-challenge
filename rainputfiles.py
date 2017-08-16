import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from epipacs import EpiPACs

STEP = 'ra_input_files'

DB = ModelConfig().jobname + ModelConfig().rundate

SQL_FILE = os.path.join(PathConfig().postec_path, 'RSPR/RA Input files.sql')

class RAInputFiles(luigi.Task):
    """ create files that are used a sinput to the risk adjustment """
    datafile = luigi.Parameter(default=STEP)

    def requires(self):
        return [EpiPACs()]

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
        print('rainputfiles.py <JOBUID>')
        exit(-1)
    luigi.run([
        'RAInputFiles', 
        '--workers', '1',
        '--local-scheduler'])
