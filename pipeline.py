import os
import luigi
import datetime
from datetime import date
from config import PathConfig
from jobsetup import JobSetup

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        setup_task = [
                JobSetup(date=self.date)
                ]
        
        tasks = setup_task
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))
