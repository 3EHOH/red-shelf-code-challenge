import luigi
import os
from config import PathConfig, PurchaseConfig
import json
from steps.purchaselistsdeduper import PurchaseListsDeduper
from steps.filereader import FileReader

STEP = 'purchaselistsorderer'


# This class is actually redundant at this point, because the order of purchases is maintained from the original list
# But, one can imagine needing this if additional steps or functionality were added, so it's anticipatory robustness diligence.

class PurchaseListsOrderer(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def order_purchase_lists(purchase_data):

        for bucket in purchase_data:
            bucket['purchases'].sort(key=lambda x: int(x.split(",")[0]))

        return purchase_data

    @staticmethod
    def requires():
        return [PurchaseListsDeduper()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        deduped_data = FileReader.read_file(PurchaseListsDeduper().datafile)
        ordered_purchase_lists = self.order_purchase_lists(deduped_data)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(ordered_purchase_lists))


if __name__ == "__main__":
    luigi.run()
