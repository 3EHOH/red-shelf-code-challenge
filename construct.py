import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, ConnieConfig, PathConfig
from run_55 import Run55 
from normalize import PostNormalize

#tasks
class Construct(luigi.contrib.external_program.ExternalProgramTask):
    """Construct"""
    jobuid = luigi.IntParameter(default=-1)
    conn_id = luigi.IntParameter(default=-1)

    def requires(self):
        return [PostNormalize(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms8G -Xmx128G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jConnie.properties control.ConstructionDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=ConnieConfig().chunksize,
            stopafter=ConnieConfig().stopafter)
        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed construction step")
        return jargs.split(' ')

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

        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed postconstructionreport step")

        return jargs.split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "postconstructionreport"))
