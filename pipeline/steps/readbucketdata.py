import luigi
import os
from config import PathConfig
import csv
import json
from steps.preflightcheck import PreflightCheck

STEP = 'readbucketdata'


class ReadBucketData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def requires():
        return [PreflightCheck()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        bucket_keys = ['publisher', 'price', 'duration']

        bucket_data = []

        with open('purchase_buckets.csv') as csvfile:
            bucket_reader = csv.reader(csvfile)
            for row in bucket_reader:
                bucket_record = {}
                for i in range(len(bucket_keys)):
                    bucket_record[bucket_keys[i]] = row[i]
                bucket_data.append(bucket_record)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(bucket_data))


if __name__ == "__main__":
    luigi.run([
        'ReadBucketData',
        '--workers', '1',
        '--local-scheduler'])