import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, PathConfig
from run_55 import Run55 
from jobsetup import JobSetup

#tasks
class Analyze(luigi.contrib.external_program.ExternalProgramTask):
    """Analyze"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [JobSetup()]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=analyze configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)
        return jargs.split(' ')

    def run(self):
        super(Analyze, self).run()
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "analyze"))
