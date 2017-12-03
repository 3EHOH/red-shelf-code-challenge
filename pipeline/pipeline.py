import os
import sys
import luigi
from config import PathConfig
from steps.preflightcheck import PreflightCheck
from steps.purchasedatareader import PurchaseDataReader
from steps.bucketdatareader import BucketDataReader
from steps.outputbucketmaker import OutputBucketMaker
from steps.sortpurchasedata import PurchaseDataBucketer
from steps.dedupepurchaselists import DedupePurchaseLists
from steps.orderpurchasebuckets import OrderPurchaseBuckets
from steps.purchaselistsorderer import PurchaseListsOrderer


class PipelineTask(luigi.WrapperTask):

    def requires(self):

        setup_tasks = [
            PreflightCheck()
        ]

        read_files = [
            PurchaseDataReader(),
            BucketDataReader()
        ]

        create_output_buckets = [
            OutputBucketMaker()
        ]

        insert_purchase_data = [
            PurchaseDataBucketer()
        ]

        dedupe_purchase_lists = [
            DedupePurchaseLists()
        ]

        sort_data = [
            PurchaseListsOrderer(),
            # OrderPurchaseBuckets()
        ]


        pipeline = setup_tasks + read_files + create_output_buckets + insert_purchase_data + dedupe_purchase_lists + sort_data

        return pipeline

    @staticmethod
    def output():
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))


if __name__ == "__main__":
    luigi.run()