import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from config import ModelConfig, ConnieConfig, PathConfig
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
