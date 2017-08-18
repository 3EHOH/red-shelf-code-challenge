import os
import sys
from time import sleep
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from config import ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from run_55 import Run55 
from ec.postmapreport import PostMapReport

STEP = 'normalization'

JARGS = 'java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
    cpath=Run55.cpath(),
    configfolder=ModelConfig().configfolder,
    chunksize=NormanConfig().chunksize,
    stopafter=NormanConfig().stopafter)

class Normalize(ExternalProgramTask):
    """ run normalization """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()
    norm_id = luigi.IntParameter()

    def requires(self):
        return [PostMapReport(jobuid=self.jobuid)]

    def program_args(self):
        return JARGS.split(' ')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(PathConfig().target_path,
                         '{}.{}'.format(self.datafile, self.norm_id)))

    def run(self):
        # HACK: it appears that the status is sometimes not properly updated
        if self.norm_id == 0:
            sql = "update processJobStep set status = 'Active' where jobUid = {jobuid} and stepName = 'normalization';".format(jobuid=self.jobuid)
            db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                         user=MySQLDBConfig().prd_user,
                                         passwd=MySQLDBConfig().prd_pass,
                                         db=MySQLDBConfig().prd_schema)    
            cur = db.cursor()
            cur.execute(sql)
            db.commit()
            db.close()

        super(Normalize, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('normalization.py <JOBUID> <NORMID>')
        exit(-1)
    luigi.run([
        'Normalize', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--norm-id', sys.argv[2],
        '--local-scheduler'])
