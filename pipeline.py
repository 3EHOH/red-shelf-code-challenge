import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
import datetime
from datetime import date
from run_55 import Run55 

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
    provider_file1=luigi.Parameter()
    member_file1=luigi.Parameter()
    enroll_file1=luigi.Parameter()

class MySQLDBConfig(luigi.Config):
    prd_schema=luigi.Parameter()
    template_schema=luigi.Parameter()
    epb_schema=luigi.Parameter()

class MongoDBConfig(luigi.Config):
    md1_schema=luigi.Parameter()

#some default params
TARGET_PATH=os.path.join(os.path.dirname(__file__),'target/{feature}{now}'.format(
    feature=ModelConfig().jobname,
    now=datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
)

#config classes
class PathConfig(luigi.Config):
    target_path=luigi.Parameter(default=TARGET_PATH)

#tasks
class JobSetup(luigi.contrib.external_program.ExternalProgramTask):
    """Setup"""

    jargs = '-Xms16G -Xmx80G -cp "{cpath}" -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna typeinput="{typeinput}" jobname="{jobname}" clientID="{clientID}" jobstep="setup" client="{client}" runname="{runname}" rundate="{rundate}" mapname="{mapname}" configfolder="{configfolder}" env="{env}" typeoutput="sql" outputPath="{outputPath}" studybegin="{studybegin}" studyend="{studyend}" metadata="{metadata}" claim_file1="{claim_file1}" claim_rx_file1="{claim_rx_file1}" provider_file1="{provider_file1}" member_file1="{member_file1}" enroll_file1="{enroll_file1}"'.format(
    cpath=Run55.cpath(),
    typeinput=ModelConfig().typeinput,
    jobname=ModelConfig().jobname,
    clientID=ModelConfig().clientID,
    client=ModelConfig().client,
    runname=ModelConfig().runname,
    rundate=ModelConfig().rundate,
    mapname=ModelConfig().mapname,
    configfolder=ModelConfig().configfolder,
    env=ModelConfig().env,
    outputPath=ModelConfig().outputPath,
    studybegin=ModelConfig().studybegin,
    studyend=ModelConfig().studyend,
    metadata=ModelConfig().metadata,
    claim_file1=ModelConfig().claim_file1,
    claim_rx_file1=ModelConfig().claim_rx_file1,
    provider_file1=ModelConfig().provider_file1,
    member_file1=ModelConfig().member_file1,
    enroll_file1=ModelConfig().enroll_file1)

    def program_args(self):
        return ['java', JobSetup.jargs]

    #def run(self):
    #    with self.output().open('w') as out_file:
    #        out_file.write(JobSetup.cmd)
    #        out_file.write("\nsuccessfully created job")

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_PATH,"create_job"))

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        setup_task = [
                JobSetup()
                ]
        
        tasks = setup_task
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_PATH,"dummy"))
