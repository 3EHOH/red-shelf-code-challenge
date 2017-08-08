import os
import luigi
from datetime import date

class ModelConfig(luigi.Config):
    typeinput=luigi.Parameter()
    jobname=luigi.Parameter()
    clientID=luigi.Parameter()
    jobstep=luigi.Parameter()
    client=luigi.Parameter()
    runname=luigi.Parameter()
    rundate=luigi.Parameter()
    mapname=luigi.Parameter()
    configfolder=luigi.Parameter()
    env=luigi.Parameter()
    typeoutput=luigi.Parameter()
    outputPath=luigi.Parameter()
    studybegin=luigi.Parameter()
    studyend=luigi.Parameter()
    metadata=luigi.Parameter()
    claim_file1=luigi.Parameter()
    claim_rx_file1=luigi.Parameter()
    member_file1=luigi.Parameter()
    enroll_file1=luigi.Parameter()

class MySQLDBConfig(luigi.Config):
    prd_schema=luigi.Parameter()
    template_schema=luigi.Parameter()
    epb_schema=luigi.Parameter()

class MongoDBConfig(luigi.Config):
    md1_schema=luigi.Parameter()

#some default params
TARGET_PATH=os.path.join(os.path.dirname(__file__),'target/{feature}{date}'.format(
    feature=ModelConfig().jobname,
    date=date.today())
)

#config classes
class PathConfig(luigi.Config):
    target_path=luigi.Parameter(default=TARGET_PATH)

#tasks
class JobSetup(luigi.Task):
    """Setup"""
    date = luigi.DateParameter()

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successfully created job")

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_PATH,"create_job"))

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        base_tasks = [
                JobSetup(date=self.date)
                ]
        
        tasks = base_tasks
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_PATH,"dummy"))
