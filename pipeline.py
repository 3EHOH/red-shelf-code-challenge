import os
import luigi
import mysql.connector
from config import ConnieConfig, ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from jobsetup import JobSetup
from analyze import Analyze
from schemacreate import SchemaCreate
from map import Map, PostMap, PostMapReport
from normalize import Normalize, PostNormalize, PostNormalizationReport
from construct import Construct, PostConstructionReport

#pipeline classes
class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = ModelConfig().rundate 
    jobuid = 1

    def requires(self):
        # HACK: we have to guess the next jobuid
        sql = "select max(uid)+1 as max_uid from processJob;"
        db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                     user=MySQLDBConfig().prd_user,
                                     passwd=MySQLDBConfig().prd_pass,
                                     db=MySQLDBConfig().prd_schema)    
        cur = db.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if len(row) == 1:
            self.jobuid = row[0]
        db.close()

        # basic setup tasks
        setup_tasks = [
            JobSetup(),
            Analyze(jobuid=self.jobuid),
            SchemaCreate(jobuid=self.jobuid)
        ]
        # mapping tasks
        map_tasks = [
                Map(jobuid=self.jobuid),
                PostMap(jobuid=self.jobuid)
                #PostMapReport(jobuid=self.jobuid)
        ] 
        # normalization tasks
        norm_ids = list(range(0, NormanConfig().count))
        norm_tasks = [Normalize(jobuid=self.jobuid, norm_id=id) for id in norm_ids]
        norm_tasks.append(PostNormalize(jobuid=self.jobuid))
        norm_tasks.append(PostNormalizationReport(jobuid=self.jobuid))

        # construction tasks
        conn_ids = list(range(0, ConnieConfig().count))
        conn_tasks = [Construct(jobuid=self.jobuid, conn_id=id) for id in conn_ids]
        conn_tasks.append(PostConstructionReport(jobuid=self.jobuid))

        # Let's go!
        tasks = setup_tasks + map_tasks + norm_tasks + conn_tasks
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))
