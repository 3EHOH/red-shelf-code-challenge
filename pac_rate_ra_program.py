import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from rainputfiles import RAInputFiles 

STEP = 'pac_rate_ra_program'

R_FILE = os.path.join(PathConfig().postec_path, 'RSPR/PAC Rate RA program.R')

class PACRateRAProgram(luigi.Task):
    """ calculate predicted probabilities to be used in risk-adjusted
        provider-level PAC rate calculations """
    datafile = luigi.Parameter(default=STEP)

    def requires(self):
        return [RAInputFiles()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):
        command = ['R', '--save']
        with open(R_FILE) as input_file:
            proc = subprocess.Popen(
                command, stdin = input_file, stderr=PIPE, stdout=PIPE)
            output,error = proc.communicate()
            with self.output().open('w') as out:
                out.write(str(error))

if __name__ == "__main__":
    luigi.run([
        'PACRateRAProgram', 
        '--workers', '1',
        '--local-scheduler'])
