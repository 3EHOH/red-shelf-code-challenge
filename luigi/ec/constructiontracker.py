import os
import luigi
import time

from utils import update_status, get_status, MySQLConn
from config import ModelConfig, PathConfig, MySQLDBConfig
from ec.constructionlauncher import ConstructionLauncher

STEP = 'constructiontracker'

class ConstructionTracker(luigi.Task):
    """ generate the post map report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [ConstructionLauncher(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):
        self.track_construction()
        self.output().open('w').close()

    @staticmethod
    def track_construction():

        select_database = "use ecr;"
        update_status(select_database)

        drop_view = "drop view if exists conview;"
        update_status(drop_view)

        create_view = "create view conview as select j.client_id, j.jobName , j.uid as jobUid, s.uid, s.sequence, s.stepName, s.description, s.status, s.updated from processJob j join processJobStep s on s.jobUid = j.uid and j.client_id = 'Test' order by j.uid, s.sequence asc;"
        update_status(create_view)

        check_is_complete = "select exists(select * from normview where stepName='construct' and status='Complete');"
        is_complete = get_status(check_is_complete)

        while not is_complete:

            query_complete = "select count(*) from jobStepQueue where jobUid=1 and stepName ='construct' and status='Complete';"
            complete_count = get_status(query_complete)

            count = 49998  #get_status("select count(*) from jobStepQueue where jobUid=1 and stepName ='construct';")

            # the count will return empty set initially hence have to make sure that this step isn't skipped immediately
            if 0 < count == complete_count > 0:
                return True
                break
            else:
                print("Current construction count: ", complete_count)
                time.sleep(60)


if __name__ == "__main__":
    luigi.run([
        'ConstructionTracker',
        '--workers', '1',
        '--local-scheduler'])
