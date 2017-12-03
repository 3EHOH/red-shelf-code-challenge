import luigi
import os
from config import PathConfig, BucketConfig
import json
from steps.orderpurchaselists import OrderPurchaseLists
from steps.readfile import ReadFile

STEP = 'orderpurchasebuckets'


class OrderPurchaseBuckets(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def order_buckets(bucket_data, output_buckets):

        ordered_set = set()

        for i, original_list_item in enumerate(bucket_data):
            for j, output_list_item in enumerate(output_buckets):
                if output_list_item['bucket'] == original_list_item:
                    print()
                    ordered_set.add(output_list_item)

        return ordered_set

    @staticmethod
    def requires():
        return [OrderPurchaseLists()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        purchase_data = []

        with open(BucketConfig().purchase_buckets) as f:
            content = f.read().splitlines()
            for row in content:
                purchase_data.append(row)

        deduped_data = ReadFile.read_file(OrderPurchaseLists().datafile)
        ordered_buckets = self.order_buckets(purchase_data, deduped_data)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(ordered_buckets))


if __name__ == "__main__":
    luigi.run()
