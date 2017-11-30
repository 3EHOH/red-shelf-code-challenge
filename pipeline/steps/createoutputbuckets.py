import luigi
import os
from config import PathConfig
import json
from steps.readbucketdata import ReadBucketData

STEP = 'createoutputbuckets'


class CreateOutputBuckets(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def requires():
        return [ReadBucketData()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        bucket_data_path = os.path.join(PathConfig().target_path, "readbucketdata")

        with open(bucket_data_path, 'r') as f:
            bucket_data = json.load(f)

        output_buckets = []

        for bucket in bucket_data:

            bucket_name = ','.join(bucket.values())
            output_buckets.append({"bucket": bucket_name, "purchases": []})

            with self.output().open('w') as out_file:
                out_file.write(json.dumps(bucket_data))


if __name__ == "__main__":
    luigi.run([
        'CreateOutputBuckets',
        '--workers', '1',
        '--local-scheduler'])