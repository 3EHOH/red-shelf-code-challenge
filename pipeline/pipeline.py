import os
import sys
import luigi
from config import PathConfig
from steps.preflightcheck import PreflightCheck
from steps.readpurchasedata import ReadPurchaseData
from steps.readbucketdata import ReadBucketData
from steps.createoutputbuckets import CreateOutputBuckets


class PipelineTask(luigi.WrapperTask):

    def requires(self):

        setup_tasks = [
            PreflightCheck()
        ]

        read_files = [
            ReadPurchaseData(),
            ReadBucketData()
        ]

        create_output_buckets = [
            CreateOutputBuckets()
        ]

        return setup_tasks + read_files + create_output_buckets

    @staticmethod
    def output():
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))


if __name__ == "__main__":
    luigi.run([
        'PipelineTask',
        '--workers', sys.argv[1],
        '--local-scheduler'])