import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
import datetime
from datetime import date
from config import ModelConfig, PathConfig
from run_55 import Run55 
from analyze import Analyze 

#tasks
class SchemaCreate(luigi.contrib.external_program.ExternalProgramTask):
    """Setup"""
    date = luigi.DateParameter(default=date.today())
    jobuid = luigi.IntParameter(default=-1)


    def requires(self):
        return [Analyze(date=self.date, jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=schemacreate configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)
        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed schemacreate step")
        return jargs.split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"schemacreate"))
