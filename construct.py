import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, ConnieConfig, PathConfig
from run_55 import Run55 
from normalize import PostNormalizationReport

#tasks
class Construct(luigi.contrib.external_program.ExternalProgramTask):
    """Construct"""
    jobuid = luigi.IntParameter(default=-1)
    conn_id = luigi.IntParameter(default=-1)

    def requires(self):
        return [PostNormalizationReport(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms8G -Xmx128G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jConnie.properties control.ConstructionDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=ConnieConfig().chunksize,
            stopafter=ConnieConfig().stopafter)
        return jargs.split(' ')

    def run(self):
        super(Construct, self).run()
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(PathConfig().target_path,
                         'construct.{}'.format(self.conn_id)))


class PostConstructionReport(luigi.contrib.external_program.ExternalProgramTask):
    """PostConstructionReport"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        conn_ids = list(range(0, ConnieConfig().count))
        return [Construct(jobuid=self.jobuid, conn_id=id) for id in conn_ids]

    def program_args(self):
        jargs = 'java -d64 -Xms4G -Xmx20G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=postconstructionreport configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)
        return jargs.split(' ')

    def run(self):
        super(PostConstructionReport, self).run()
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "postconstructionreport"))
