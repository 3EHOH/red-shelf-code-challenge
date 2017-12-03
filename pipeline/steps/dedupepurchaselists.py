import luigi
import os
from config import PathConfig
import json
from steps.sortpurchasedata import SortPurchaseData

STEP = 'dedupepurchasedata'

class DedupePurchaseLists(luigi.Task):
    datafile = luigi.Parameter(default=STEP)


    def dedupe_purchase_lists(self, output_buckets):

        unique_buckets = set()

        for bucket in output_buckets:
            if bucket['bucket'] not in unique_buckets:
                unique_buckets.add(bucket['bucket'])
            else:
                bucket['purchases'] = []

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

        output_buckets = self.read_files("sortpurchasedata")

        self.dedupe_purchase_lists(self, output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run()
