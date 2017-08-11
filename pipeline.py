import os
import luigi
import datetime
from datetime import date
from config import PathConfig
from jobsetup import JobSetup
from analyze import Analyze
from schemacreate import SchemaCreate
from map import Map
from postmap import PostMap

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = luigi.DateParameter(default=date.today())
    jobuid = 28

    def requires(self):
        setup_tasks = [
            JobSetup(date=self.date),
            Analyze(date=self.date, jobuid=self.jobuid),
            SchemaCreate(date=self.date, jobuid=self.jobuid)
        ]
        map_tasks = [
                Map(date=self.date, jobuid=self.jobuid),
                PostMap(date=self.date, jobuid=self.jobuid)
        ]        

        tasks = setup_tasks + map_tasks
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))
