import os
import sys
import luigi
import mysql.connector
import subprocess
from subprocess import Popen, PIPE

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from ec.post.optional.pac_rate_ra_program import PACRateRAProgram 

STEP = 'provider_pac_rates'

DB = ModelConfig().jobname + ModelConfig().rundate

SQL_FILE = os.path.join(PathConfig().postec_path,
                        'RSPR/Provider_PAC_Rates table.sql')

class ProviderPACRates(luigi.Task):
    """ create provider-level aggregates """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.Parameter()

    def requires(self):
        return [PACRateRAProgram(jobuid=self.jobuid)]

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
        'ProviderPACRates', 
        '--workers', '1',
        '--jobuid', sys.argv[1], 
        '--local-scheduler'])
