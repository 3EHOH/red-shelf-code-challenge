import luigi
import os
from config import PathConfig
import json
from steps.purchasedatabucketer import PurchaseDataBucketer
from steps.filereader import FileReader

STEP = 'purchaselistsdeduper'

# This class empties the purchase data from duplicate buckets, as per the README instruction that only the first occurrence
# of a bucket should have the data. This is actually redundant at this point, because we use a generator to ensure that
# duplicate buckets aren't inserted into if a first one is found. But, a good engineer can imagine needing this if
# additional steps or functionality were added, so it's anticipatory robustness diligence.

class PurchaseListsDeduper(luigi.Task):
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
        return [PurchaseDataBucketer()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        output_buckets = FileReader.read_file(PurchaseDataBucketer().datafile)
        deduped_data = self.dedupe_purchase_lists(output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(deduped_data))


if __name__ == "__main__":
    luigi.run()
