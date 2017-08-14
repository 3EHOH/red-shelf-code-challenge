import os
import sys
import luigi
from config import ConnieConfig, ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from setup import Setup
from analyze import Analyze
from schemacreate import SchemaCreate
from map import Map
from postmap import PostMap
from postmapreport import PostMapReport
from normalization import Normalize
from postnormalization import PostNormalize
from postnormalizationreport import PostNormalizationReport
from construct import Construct
from postconstructionreport import PostConstructionReport

class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = ModelConfig().rundate 
    jobuid = ModelConfig().jobuid 

    def requires(self):
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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('pipeline.py <workers>')
        exit(-1)
    luigi.run([
        'PipelineTask', 
        '--workers', sys.argv[1],
        '--local-scheduler'])
