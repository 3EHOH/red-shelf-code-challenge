import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

import mysql.connector

from config import ModelConfig, ConnieConfig, PathConfig, MySQLDBConfig
from run_55 import Run55 
from ec.construction import Construct

STEP = 'postconstructionreport'

JARGS = 'java -d64 -Xms4G -Xmx20G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)

class PostConstructionReport(ExternalProgramTask):
    """ generate the post construction report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        conn_ids = list(range(0, ConnieConfig().count))
        return [Construct(jobuid=self.jobuid, conn_id=id) for id in conn_ids]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    def run(self):
        # HACK: it appears that sometimes the construction status is not
        # updated correctly
        sql = "update processJobStep set status = 'Complete' where jobUid = {jobuid} and stepName = 'construct';".format(jobuid=self.jobuid)
        db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                     user=MySQLDBConfig().prd_user,
                                     passwd=MySQLDBConfig().prd_pass,
                                     db=MySQLDBConfig().prd_schema)    
        cur = db.cursor()
        cur.execute(sql)
        db.commit()
        db.close()
        super(PostConstructionReport, self).run()
        self.output().open('w').close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('postconstructionreport.py <JOBUID>')
        exit(-1)
    luigi.run([
        'PostConstructionReport', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
