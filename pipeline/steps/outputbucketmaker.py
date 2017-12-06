import luigi
import os
from config import PathConfig
import json
from steps.bucketdatareader import BucketDataReader

STEP = 'outputbucketmaker'

# Create the output bucket dictionaries based on the bucket csv input data.


class OutputBucketMaker(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def make_output_buckets(data_path):
        output_buckets = []

        with open(data_path, 'r') as f:
            bucket_data = json.load(f)

        for bucket in bucket_data:
            bucket_name = ','.join(bucket.values())
            output_buckets.append({"bucket": bucket_name, "purchases": []})

        return output_buckets

    @staticmethod
    def requires():
        return [BucketDataReader()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        bucket_data_path = os.path.join(PathConfig().target_path, BucketDataReader().datafile)
        output_buckets = self.make_output_buckets(bucket_data_path)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run()