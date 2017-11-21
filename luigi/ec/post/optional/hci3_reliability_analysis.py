import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE
from logutils import LogUtils
from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from ec.post.optional.provider_pac_rates import ProviderPACRates

STEP = 'hci3_reliability_analysis'

R_FILE = os.path.join(PathConfig().postec_path,
                      'RSPR/HCI3_Reliability_Analysis.R')

class HCI3ReliabilityAnalysis(luigi.Task):
    """ perform an HCI3 reliability analysis """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [ProviderPACRates(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):
        LogUtils.log_start(STEP)
        command = ['R', '--save']
        with open(R_FILE) as input_file:
            proc = subprocess.Popen(
                command, stdin = input_file, stderr=PIPE, stdout=PIPE)
            output,error = proc.communicate()
            with self.output().open('w') as out:
                out.write(str(error))
        LogUtils.log_stop(STEP)


if __name__ == "__main__":
    luigi.run([
        'HCI3ReliabilityAnalysis', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
