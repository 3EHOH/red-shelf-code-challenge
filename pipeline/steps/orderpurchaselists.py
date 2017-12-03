import luigi
import os
from config import PathConfig, PurchaseConfig
import json
from steps.dedupepurchaselists import DedupePurchaseLists
from steps.readfile import ReadFile

STEP = 'orderpurchaselists'


class OrderPurchaseLists(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def order_purchases(purchase_data):
        return sorted(purchase_data, lambda x: int(x.split()[-1]))

    @staticmethod
    def requires():
        return [DedupePurchaseLists()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        deduped_data = ReadFile.read_file("dedupepurchaselists")
        ordered_purchase_lists = self.order_buckets(deduped_data)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(ordered_purchase_lists))


if __name__ == "__main__":
    luigi.run()
