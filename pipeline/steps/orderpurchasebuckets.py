import luigi
import os
from config import PathConfig, BucketConfig
import json
import csv
from steps.dedupepurchaselists import DedupePurchaseLists
from steps.readfile import ReadFile

STEP = 'orderpurchasebuckets'


class OrderPurchaseBuckets(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def order_buckets(bucket_data, output_buckets):
        return sorted(output_buckets, key=lambda item: bucket_data.index(item['bucket']))

    @staticmethod
    def requires():
        return [DedupePurchaseLists()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        purchase_data = []

        with open(BucketConfig().purchase_buckets, 'r') as f:
            reader = csv.reader(f)
            purchase_data.append(reader)

        print(purchase_data)

        deduped_data = ReadFile.read_file("dedupepurchaselists")

        ordered_buckets = self.order_buckets(purchase_data, deduped_data)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(ordered_buckets))


if __name__ == "__main__":
    luigi.run()
