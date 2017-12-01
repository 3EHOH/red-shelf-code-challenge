import luigi
import os
from config import PathConfig, BucketConfig
import csv
import json
from steps.preflightcheck import PreflightCheck

STEP = 'readdata'


class ReadData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self, csv_file, csv_file_keys):
        self.csv_file = csv_file
        self.csv_file_keys = csv_file_keys
        luigi.Task.__init__()

    @staticmethod
    def requires():
        return [PreflightCheck()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        purchase_buckets_file = self.csv_file
        bucket_keys = self.csv_file_key
        bucket_data = []

        with open(purchase_buckets_file) as csvfile:
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
        'ReadData',
        '--workers', '1',
        '--local-scheduler'])