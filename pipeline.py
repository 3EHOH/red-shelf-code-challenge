import os
import luigi
import mysql.connector
from config import ModelConfig, PathConfig
from jobsetup import JobSetup
from analyze import Analyze
from schemacreate import SchemaCreate
from map import Map
from postmap import PostMap

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = ModelConfig().rundate 
    jobuid = 1

    def requires(self):
        # HACK: we have to guess the next jobuid
        sql = "select max(uid)+1 as max_uid from processJob;"
        db = mysql.connector.connect(host="localhost", user="root", passwd="hackers123", db="ecr")
        cur = db.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if len(row) == 1:
            self.jobuid = row[0]
        db.close()

        setup_tasks = [
            JobSetup(date=self.date),
            Analyze(date=self.date, jobuid=self.jobuid),
            SchemaCreate(date=self.date, jobuid=self.jobuid)
        ]
        map_tasks = [
                Map(date=self.date, jobuid=self.jobuid),
                PostMap(date=self.date, jobuid=self.jobuid)
        ]        

        tasks = setup_tasks + map_tasks
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))
