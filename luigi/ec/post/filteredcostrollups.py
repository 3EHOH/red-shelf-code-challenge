import os
import sys
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from utils import update_status
from logutils import LogUtils
from config import ModelConfig, MySQLDBConfig, PathConfig
from run_55 import Run55 
from ec.post.costrollups import CostRollUps

STEP = 'filteredcostrollups'

JARGS = 'java -d64 -Xms4G -Xmx200G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jOtto.properties control.BigKahuna jobstep={jobstep} configfolder={configfolder}'.format(
    cpath=Run55.cpath(),
    jobstep=STEP,
    configfolder=ModelConfig().configfolder)


class FilteredCostRollUps(ExternalProgramTask):
    """ do the filtered cost roll-ups """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [CostRollUps(jobuid=self.jobuid)]

    def program_args(self):
        return '{} jobuid={}'.format(JARGS, self.jobuid).split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        # update the status
        sql = "update processJobStep set status = 'Active', stepStart = now() where jobUid = {jobuid} and stepName = 'filteredrollups';".format(jobuid=self.jobuid)
        update_status(sql)
        LogUtils.log_start(STEP)
        # run the SQL script
        super(FilteredCostRollUps, self).run()
        self.output().open('w').close()
        LogUtils.log_stop(STEP)
        # update the status
        sql = "update processJobStep set status = 'Complete', stepEnd = now() where jobUid = {jobuid} and stepName = 'filteredrollups';".format(jobuid=self.jobuid)
        update_status(sql)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('filteredcostrollups.py <JOBUID>')
        exit(-1)
    luigi.run([
        'FilteredCostRollUps', 
        '--workers', '1',
        '--jobuid', sys.argv[1],
        '--local-scheduler'])
