import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from config import ModelConfig, NormanConfig, PathConfig
from run_55 import Run55 
from map import PostMap 

#tasks
class Normalize(luigi.contrib.external_program.ExternalProgramTask):
    """Normalize"""
    jobuid = luigi.IntParameter(default=-1)
    norm_id = luigi.IntParameter(default=-1)

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=NormanConfig().norman_chunksize,
            stopafter=NormanConfig().norman_stopafter)
        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed normalization step")
        return jargs.split(' ')

    def output(self):
        return luigi.LocalTarget(
            os.path.join(PathConfig().target_path,
                         'normalize.{}'.format(self.norm_id)))


class PostNormalize(luigi.contrib.external_program.ExternalProgramTask):
    """PostNormalize"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        norm_ids = list(range(0, NormConfig().norman_count))
        return [Normalize(jobuid=self.jobuid, norm_id=id) for id in norm_ids]

    def program_args(self):
        jargs = 'java -d64 -Xms4G -Xmx20G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=postnormalization configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)

        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed postnormalization step")

        return jargs.split(' ')

    def output(self):
        # HACK: set the construction status to READY.
        sql = "update processJobStep set status = 'Ready' where jobUid = {jobuid} and stepName = 'construct';".format(jobuid=self.jobuid)

        db = mysql.connector.connect(host="localhost", user="root",
                                     passwd="hackers123", db="ecr")    
        cur = db.cursor()
        cur.execute(sql)
        db.commit()
        db.close()

        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "postnormalization"))

