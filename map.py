import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
import mysql.connector
from config import ModelConfig, MySQLDBConfig, PathConfig
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
        return jargs.split(' ')

    def run(self):
        super(Map, self).run()
        self.output().open('w').close()

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
        return jargs.split(' ')

    def run(self):
        super(PostMap, self).run()
        self.output().open('w').close()
        # HACK: set the normalization status to READY.
        sql = "update processJobStep set status = 'Ready' where jobUid = {jobuid} and stepName = 'normalization';".format(jobuid=self.jobuid)
        db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                     user=MySQLDBConfig().prd_user,
                                     passwd=MySQLDBConfig().prd_pass,
                                     db=MySQLDBConfig().prd_schema)    
        cur = db.cursor()
        cur.execute(sql)
        db.commit()
        db.close()

    def output(self):
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
        return jargs.split(' ')

    def run(self):
        super(PostMapReport, self).run()
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"postmapreport"))
