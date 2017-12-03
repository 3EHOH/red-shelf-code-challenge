import luigi
import os
from config import PathConfig
import json
from steps.sortpurchasedata import SortPurchaseData
from steps.readfile import ReadFile

STEP = 'dedupepurchaselists'


class DedupePurchaseLists(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def dedupe_purchase_lists(output_buckets):

        unique_buckets = set()

        for bucket in output_buckets:
            if bucket['bucket'] not in unique_buckets:
                unique_buckets.add(bucket['bucket'])
            else:
                bucket['purchases'] = []

        return output_buckets

    @staticmethod
    def requires():
        return [SortPurchaseData()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        output_buckets = ReadFile.read_file("sortpurchasedata")
        deduped_data = self.dedupe_purchase_lists(output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(deduped_data))


if __name__ == "__main__":
    luigi.run()
