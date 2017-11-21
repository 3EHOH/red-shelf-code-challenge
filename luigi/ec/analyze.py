import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from logutils import LogUtils
from config import ModelConfig, PathConfig
from run_55 import Run55 
from ec.setup import Setup

STEP = 'analyze'

JARGS = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class Analyze(ExternalProgramTask):
    """ analyze the input data """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [Setup()]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        LogUtils.log_start(STEP)
        super(Analyze, self).run()
        self.output().open('w').close()
        LogUtils.log_stop(STEP)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('analyze.py <JOBUID>')
        exit(-1)
    luigi.run([
        'Analyze', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
