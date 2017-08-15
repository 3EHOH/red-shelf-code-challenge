import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from provider_pac_rates import ProviderPACRates

STEP = 'hci3_reliability_analysis'

R_FILE = os.path.join(PathConfig().postec_path,
                      'RSPR/HCI3_Reliability_Analysis.R')

class HCI3ReliabilityAnalysis(luigi.Task):
    """ perform an HCI3 reliability analysis """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [ProviderPACRates(jobuid=self.jobuid)]

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
    if len(sys.argv) < 2:
        print('hci3_reliability_analysis.py <JOBUID>')
        exit(-1)
    luigi.run([
        'HCI3ReliabilityAnalysis', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
