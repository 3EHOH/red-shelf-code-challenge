import luigi
import os
from config import PathConfig
import json
from steps.createoutputbuckets import CreateOutputBuckets

STEP = 'sortpurchasedata'


class SortPurchaseData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)



    @staticmethod
    def sort_data(self, purchase_data, bucket_data, output_buckets):

        no_duplicates = []

        for record in purchase_data:

            record_values = ','.join(record.values())

            for bucket in bucket_data:

                if record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price'] and \
                                record['duration'].lower() == bucket['duration'].lower():

                    compare = record_values
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                elif record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price']:

                    compare = record['publisher'].lower() + "," + record['price'] + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                elif record['price'].lower() == bucket['price'].lower() and record['duration'] == bucket['duration']:

                    compare = "*" + "," + record['price'] + "," + record['duration']
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                elif record['publisher'].lower() == bucket['publisher'].lower():

                    compare = record['publisher'].lower() + "," + "*" + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                elif record['price'].lower() == bucket['price'].lower():

                    compare = "*" + "," + record['price'] + "," + "*"
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                elif record['duration'].lower() == bucket['duration'].lower():

                    compare = record['publisher'].lower() + "," + "*" + "," + record['duration']
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

                else:
                    compare = "*,*,*"
                    self.find_and_assign(compare, output_buckets, record_values, no_duplicates)

    @staticmethod
    def find_and_assign(compare, output_buckets, record_values, no_duplicates):
        for i, dic in enumerate(output_buckets):
            if dic['bucket'].lower() == compare and record_values not in dic['purchases'] and not \
                    any(d['bucket_name'].lower() == compare and d['purchase'] == record_values for d in no_duplicates):
                dic['purchases'].append(record_values)
                no_duplicates.append({"bucket_name": compare, "purchase": record_values})

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

        output_buckets = self.read_files("createoutputbuckets")
        purchase_data = self.read_files("readpurchasedata")
        bucket_data = self.read_files("readbucketdata")

        self.sort_data(self, purchase_data, bucket_data, output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run([
        'SortPurchaseData',
        '--workers', '1',
        '--local-scheduler'])
