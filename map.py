import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
import mysql.connector
from config import ModelConfig, PathConfig
from run_55 import Run55 
from schemacreate import SchemaCreate 

#tasks
class Map(luigi.contrib.external_program.ExternalProgramTask):
    """Map"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [SchemaCreate(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=map configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)

        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed map step")

        return jargs.split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"map"))


class PostMap(luigi.contrib.external_program.ExternalProgramTask):
    """PostMap"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [Map(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=postmap configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)

        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed postmap step")

        return jargs.split(' ')

    def output(self):
        # HACK: set the normalization status to READY.
        sql = "update processJobStep set status = 'Ready' where jobUid = {jobuid} and stepName = 'normalization';".format(jobuid=self.jobuid)

        db = mysql.connector.connect(host="localhost", user="root",
                                     passwd="hackers123", db="ecr")    
        cur = db.cursor()
        cur.execute(sql)
        db.commit()
        db.close()

        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              "postmap"))


class PostMapReport(luigi.contrib.external_program.ExternalProgramTask):
    """PostMapReport"""
    jobuid = luigi.IntParameter(default=-1)

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def program_args(self):
        jargs = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna jobstep=postmapreport configfolder={configfolder} jobuid={jobuid}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            jobuid=self.jobuid)

        with self.output().open('w') as out_file:
            out_file.write(jargs)
            out_file.write("\nsuccessfully completed postmapreport step")

        return jargs.split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"postmapreport"))

