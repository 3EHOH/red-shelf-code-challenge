import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from ec.post.optional.ieva import IEVA

STEP = 'pac_super_groups'

DB = ModelConfig().jobname + ModelConfig().rundate

SQL_FILE = os.path.join(PathConfig().postec_path,
                        'pac_super_groups.sql')

class PACSuperGroups(luigi.Task):
    """ consolidate PAC code groups into PAC Super Groups """
    datafile = luigi.Parameter(default=STEP)

    def requires(self):
        return [IEVA()]

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
        'PACSuperGroups', 
        '--workers', '1',
        '--local-scheduler'])
