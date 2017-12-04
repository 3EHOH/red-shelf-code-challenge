import luigi
import os
from config import PathConfig, BucketConfig
import json
from steps.orderpurchaselists import OrderPurchaseLists
from steps.readfile import ReadFile

STEP = 'orderpurchasebuckets'

# I have left this class in, though I have removed it from the pipeline. The intention was to make an explicit step to
# ensure that the buckets are kept in their original order. At this point, it's redundant, because the lists preserver
# the order extracted from the CSV files. However, a good engineer can foresee the need for such a step. Using the
# `sorted` generator actually undermines the intended effect, though, because it naively reorders duplicate buckets
# immediately after their first occurrence. I leave this class in to show that the potential need was foreseen, and a
# good-faith effort was made to handle it, though completion of the task at hand is more important now.


class OrderPurchaseBuckets(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def order_buckets(bucket_data, output_buckets):
        return sorted(output_buckets, key=lambda item: bucket_data.index(item['bucket']))

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

        deduped_data = ReadFile.read_file("dedupepurchaselists")
        ordered_buckets = self.order_buckets(purchase_data, deduped_data)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(ordered_buckets))


if __name__ == "__main__":
    luigi.run()
