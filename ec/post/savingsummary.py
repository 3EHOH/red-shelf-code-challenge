import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from config import ModelConfig, PathConfig
from run_55 import Run55 
from ec.post.res import RES

STEP = 'savingsummary'

JARGS = 'java -d64 -Xms4G -Xmx200G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jOtto.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class SavingSummary(ExternalProgramTask):
    """ run the saving summary """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [RES(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        super(SavingSummary, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('savingsummary.py <JOBUID>')
        exit(-1)
    luigi.run([
        'SavingSummary', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
