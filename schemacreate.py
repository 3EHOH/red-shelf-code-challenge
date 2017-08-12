import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, PathConfig
from run_55 import Run55 
from analyze import Analyze 

#tasks
class SchemaCreate(luigi.contrib.external_program.ExternalProgramTask):
    """SchemaCreate"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [Analyze(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=schemacreate configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)
        return jargs.split(' ')

    def run(self):
        super(SchemaCreate, self).run()
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "schemacreate"))
