import os
import luigi
from utils import update_status
from config import ModelConfig, PathConfig
from ec.normlauncher import NormLauncher

STEP = 'normtracker'

class NormTracker(luigi.Task):
    """ generate the post map report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [NormLauncher(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):
        self.track_norman()
        self.output().open('w').close()

    @staticmethod
    def track_norman():
        select_database = "use ecr;"
        update_status(select_database)
        drop_view = "drop view if exists normview;"
        update_status(drop_view)

        # creating view or ecr table named normview for reference

        create_view = "create view normview as select j.client_id, j.jobName , j.uid as jobUid, s.uid, s.sequence, s.stepName, s.description, s.status, s.updated from processJob j join processJobStep s on s.jobUid = j.uid and j.client_id = 'Test' order by j.uid, s.sequence asc;"
        update_status(create_view)

        check_is_complete = "select exists(select * from normview where stepName='normalization' and status='Complete');"
        cursor = update_status(check_is_complete)
        is_complete = cursor.fetchone()[0]

        while not is_complete:

            # sql3 = "select exists(select * from normview where stepName='normalization' and status='Complete');"

            sql4 = "select count(1) from jobStepQueue where jobUid=1 and stepName ='normalization' and status='Complete';"

            cursor = update_status(sql4)
            complete_count = cursor.fetchone()[0]

            sql5 = "select count(1) from jobStepQueue where jobUid=1 and stepName ='normalization';"

            cursor = update_status(sql5)
            count = cursor.fetchone()[0]

            # the count will return empty set initially hence have to make sure that this step isn't skipped immediately
            if 0 < count == complete_count > 0:
                return True
                break
            else:
                # till norm completes show the current count
                print("Current Normalization count->" + count)


if __name__ == "__main__":
    luigi.run([
        'NormTracker',
        '--workers', '1',
        '--local-scheduler'])
