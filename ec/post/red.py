import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from ...config import ModelConfig, PathConfig
from ...run_55 import Run55 
from rasamodel import RASAModel

STEP = 'red'

JARGS = 'java -d64 -Xms4G -Xmx200G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jOtto.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class RED(ExternalProgramTask):
    """ run the episode detail report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [RASAModel(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        super(RED, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('red.py <JOBUID>')
        exit(-1)
    luigi.run([
        'RED', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
