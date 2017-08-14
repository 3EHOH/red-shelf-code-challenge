import os
import sys
import luigi
import mysql.connector

from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from epidedupe import Dedupe

STEP = 'providerattribution'

DB = ModelConfig().job_name + ModelConfig.run_date

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
        with open(SQL_FILE, "r") as fp:
            sql = "".join(fp.readlines()[0:])
            db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                         user=MySQLDBConfig().prd_user,
                                         passwd=MySQLDBConfig().prd_pass,
                                         db=DB)    
            cur = db.cursor()
            cur.execute(sql)
            db.commit()
            db.close()

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
