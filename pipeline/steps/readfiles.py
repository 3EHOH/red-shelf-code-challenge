import luigi
import os
from config import PathConfig
import csv
import json
from steps.preflightcheck import PreflightCheck

STEP = 'readfiles'


class ReadFiles(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    def requires(self):
        return [PreflightCheck(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        purchase_keys = ['order_id', 'isbn', 'publisher', 'school', 'price', 'duration', 'order_datetime']

        bucket_keys = ['publisher', 'price', 'duration']

        purchase_data = []

        bucket_data = []

        with open('purchase_data.csv') as csvfile:
            purchase_reader = csv.reader(csvfile)
            for row in purchase_reader:
                purchase_record = {}
                for i in range(len(purchase_keys)):
                    purchase_record[purchase_keys[i]] = row[i]
                purchase_data.append(purchase_record)

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
        'ReadFiles',
        '--workers', '1',
        '--local-scheduler'])