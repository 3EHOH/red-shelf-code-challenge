import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, PathConfig
from run_55 import Run55
from ec.postmap import PostMap
from logutils import LogUtils


STEP = 'postmapreport'

JARGS = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)

class PostMapReport(ExternalProgramTask):
    """ generate the post map report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    def run(self):
        LogUtils.log_start(STEP)
        super(PostMapReport, self).run()
        self.output().open('w').close()
        LogUtils.log_stop(STEP)        


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('postmapreport.py <JOBUID>')
        exit(-1)
    luigi.run([
        'PostMapReport', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
