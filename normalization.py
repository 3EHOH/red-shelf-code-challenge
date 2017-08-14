import os
from random import randint
from time import sleep
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from config import ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from run_55 import Run55 
from postmap import PostMap

STEP = 'normalization'

JARGS = 'java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
    cpath=Run55.cpath(),
    configfolder=ModelConfig().configfolder,
    chunksize=NormanConfig().chunksize,
    stopafter=NormanConfig().stopafter)

class Normalize(ExternalProgramTask):
    """ run normalization """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter(default=-1)
    norm_id = luigi.IntParameter(default=-1)

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def program_args(self):
        return JARGS.split(' ')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(PathConfig().target_path,
                         '{}.{}'.format(self.datafile, self.norm_id)))

    def run(self):
        sleep(randint(5,20))
        super(Normalization, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('normalization.py <JOBUID> <NORMID>')
        exit(-1)
    luigi.run([
        'Normalize', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--norm_id', sys.argv[2],
        '--local-scheduler'])
