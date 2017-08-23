import os
import sys
from time import sleep
import luigi
from luigi.contrib.external_program import ExternalProgramTask

from config import ModelConfig, MySQLDBConfig, ConnieConfig, PathConfig
from run_55 import Run55 
from ec.postnormalizationreport import PostNormalizationReport

STEP = 'construction'

JARGS = 'java -d64 -Xms8G -Xmx128G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jConnie.properties control.ConstructionDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
    cpath=Run55.cpath(),
    configfolder=ModelConfig().configfolder,
    chunksize=ConnieConfig().chunksize,
    stopafter=ConnieConfig().stopafter)

class Construct(ExternalProgramTask):
    """ run construction """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()
    conn_id = luigi.IntParameter()

    def requires(self):
        return [PostNormalizationReport(jobuid=self.jobuid)]

    def program_args(self):
        return JARGS.split(' ')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(PathConfig().target_path,
                         '{}.{}'.format(self.datafile, self.conn_id)))

    def run(self):
        sleep(600*self.conn_id)
        super(Construct, self).run()
        self.output().open('w').close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('construction.py <JOBUID> <CONNID>')
        exit(-1)
    luigi.run([
        'Construct', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--conn-id', sys.argv[2],
        '--local-scheduler'])
