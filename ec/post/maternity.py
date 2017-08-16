import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from ...config import ModelConfig, PathConfig
from ...run_55 import Run55 
from pasanalysis import PASAnalysis 

STEP = 'maternity'

JARGS = 'java -d64 -Xms4G -Xmx200G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jOtto.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class Maternity(ExternalProgramTask):
    """ run the maternity and NBORN reporting filters """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [PASAnalysis(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        super(Maternity, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('maternity.py <JOBUID>')
        exit(-1)
    luigi.run([
        'Maternity', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
