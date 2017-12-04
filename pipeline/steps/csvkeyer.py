import luigi
import os
from config import PathConfig, BucketConfig
import csv
import json
from steps.preflightcheck import PreflightCheck

# This class itself isn't actually part of the pipeline
# but PurchaseDataReader and BucketDataReader are subclasses of it.


class CsvKeyer(luigi.Task):

    def __init__(self, csv_file, csv_file_keys):
        self.csv_file = csv_file
        self.csv_file_keys = csv_file_keys
        luigi.Task.__init__(self)

    @staticmethod
    def add_keys_to_data(csv_file, keys):
        bucket_data = []

        with open(csv_file) as f:
            bucket_reader = csv.reader(f)
            for row in bucket_reader:
                bucket_record = {}
                for i in range(len(keys)):
                    bucket_record[keys[i]] = row[i]
                bucket_data.append(bucket_record)

        return bucket_data

    @staticmethod
    def requires():
        return [PreflightCheck()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        csv_file = self.csv_file
        keys = self.csv_file_keys
        data = self.add_keys_to_data(csv_file, keys)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(data))


if __name__ == "__main__":
    luigi.run()