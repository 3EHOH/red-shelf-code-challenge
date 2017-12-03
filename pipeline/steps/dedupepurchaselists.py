import luigi
import os
from config import PathConfig
import json
from steps.sortpurchasedata import SortPurchaseData

STEP = 'dedupepurchasedata'

class DedupePurchaseLists(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def sort_data(self, purchase_data):

        for record in purchase_data:


    @staticmethod
    def find_and_assign(compare, output_buckets, record_values, unique_buckets_and_purchases):
        for i, dic in enumerate(output_buckets):
            if dic['bucket'].lower() == compare and record_values not in dic['purchases'] and not \
                    any(d['bucket_name'].lower() == compare and d['purchase'] == record_values for d in
                        unique_buckets_and_purchases):
                dic['purchases'].append(record_values)
                unique_buckets_and_purchases.append({"bucket_name": compare, "purchase": record_values})


    @staticmethod
    def requires():
        return [SortPurchaseData()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    @staticmethod
    def read_files(pathname):
        path_to_file = os.path.join(PathConfig().target_path, pathname)

        with open(path_to_file, 'r') as f:
            file_output = json.load(f)

        return file_output

    def run(self):

        output_buckets = self.read_files(SortPurchaseData.STEP)

        self.dedupe_purchase_lists(self, output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run()
