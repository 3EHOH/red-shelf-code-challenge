import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from utils import update_status

from config import ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from run_55 import Run55 
from ec.normalization import Normalize

STEP = 'postnormalization'

JARGS = 'java -d64 -Xms4G -Xmx20G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)

class PostNormalize(ExternalProgramTask):
    """ run post normalization task """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        norm_ids = list(range(0, NormanConfig().count))
        return [NormLauncher(jobuid=self.jobuid] #replace this with NormTracker

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    def run(self):
        # HACK: it appears that sometimes the normalization status is not
        # updated correctly
        sql = "update processJobStep set status = 'Complete' where jobUid = {jobuid} and stepName = 'normalization';".format(jobuid=self.jobuid)
        update_status(sql)
        super(PostNormalize, self).run()
        # HACK: set the construction status to READY.
        sql = "update processJobStep set status = 'Ready' where jobUid = {jobuid} and stepName = 'construct';".format(jobuid=self.jobuid)
        update_status(sql)
        self.output().open('w').close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('postnormalization.py <JOBUID>')
        exit(-1)
    luigi.run([
        'PostNormalize', 
        '--workers', '8',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
