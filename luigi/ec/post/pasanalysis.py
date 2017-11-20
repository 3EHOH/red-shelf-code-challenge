import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from logutils import LogUtils
from config import ModelConfig, PathConfig
from run_55 import Run55 
from ec.post.pacanalysis import PACAnalysis 

STEP = 'pasanalysis'

JARGS = 'java -d64 -Xms4G -Xmx200G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jOtto.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class PASAnalysis(ExternalProgramTask):
    """ run the potentially avoidable service detail """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [PACAnalysis(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        LogUtils.log_start(STEP)
        super(PASAnalysis, self).run()
        self.output().open('w').close()
        LogUtils.log_stop(STEP)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('pasanalysis.py <JOBUID>')
        exit(-1)
    luigi.run([
        'PASAnalysis', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
