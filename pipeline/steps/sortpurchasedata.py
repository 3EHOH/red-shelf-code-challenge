import luigi
import os
from config import PathConfig
import json
from steps.createoutputbuckets import CreateOutputBuckets

STEP = 'sortpurchasedata'


class SortPurchaseData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def find_and_assign(compare, output_buckets, record_values):
        for i, dic in enumerate(output_buckets):
            if dic['bucket'].lower() == compare and record_values not in dic['purchases']:
                dic['purchases'].append(record_values)

    @staticmethod
    def requires():
        return [CreateOutputBuckets()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    @staticmethod
    def read_files(pathname):
        path_to_file = os.path.join(PathConfig().target_path, pathname)

        with open(path_to_file, 'r') as f:
            file_output = json.load(f)

        return file_output

    def run(self):

        # output_buckets_path = os.path.join(PathConfig().target_path, "createoutputbuckets")
        #
        # with open(output_buckets_path, 'r') as f:
        #     output_buckets = json.load(f)

        output_buckets = self.read_files("createoutputbuckets")
        purchase_data = self.read_files("readpurchasedata")
        bucket_data = self.read_files("readbucketdata")

        # purchase_data_path = os.path.join(PathConfig().target_path, "readpurchasedata")
        #
        # with open(purchase_data_path, 'r') as f:
        #     purchase_data = json.load(f)
        #
        # bucket_data_path = os.path.join(PathConfig().target_path, "readbucketdata")
        #
        # with open(bucket_data_path, 'r') as f:
        #     bucket_data = json.load(f)

        for record in purchase_data:

            record_values = ','.join(record.values())

            for bucket in bucket_data:

                if record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price'] and \
                                record['duration'].lower() == bucket['duration'].lower():

                    compare = record_values
                    self.find_and_assign(compare, output_buckets, record_values)

                elif record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price']:

                    compare = record['publisher'].lower() + "," + record['price'] + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values)

                elif record['price'].lower() == bucket['price'].lower() and record['duration'] == bucket['duration']:

                    compare = "*" + "," + record['price'] + "," + record['duration']
                    self.find_and_assign(compare, output_buckets, record_values)

                elif record['publisher'].lower() == bucket['publisher'].lower():

                    compare = record['publisher'].lower() + "," + "*" + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values)

                elif record['price'].lower() == bucket['price'].lower():

                    compare = "*" + "," + record['price'] + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values)

                elif record['duration'].lower() == bucket['duration'].lower():

                    compare = record['publisher'].lower() + "," + "*" + "," + record['duration']
                    self.find_and_assign(compare, output_buckets, record_values)

                else:
                    compare = "*,*,*"
                    self.find_and_assign(compare, output_buckets, record_values)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run([
        'SortPurchaseData',
        '--workers', '1',
        '--local-scheduler'])