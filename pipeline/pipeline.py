import os
import sys
import luigi
from config import PathConfig
from steps.preflightcheck import PreflightCheck
from steps.readpurchasedata import ReadPurchaseData
from steps.readbucketdata import ReadBucketData
from steps.createoutputbuckets import CreateOutputBuckets
from steps.sortpurchasedata import SortPurchaseData
from steps.dedupepurchaselists import DedupePurchaseLists
from steps.orderpurchasebuckets import OrderPurchaseBuckets
from steps.orderpurchaselists import OrderPurchaseLists


class PipelineTask(luigi.WrapperTask):

    def requires(self):

        setup_tasks = [
            PreflightCheck()
        ]

        read_files = [
            ReadPurchaseData(),
            ReadBucketData()
        ]

        create_output_buckets = [
            CreateOutputBuckets()
        ]

        insert_purchase_data = [
            SortPurchaseData()
        ]

        dedupe_purchase_lists = [
            DedupePurchaseLists()
        ]

        sort_data = [
            OrderPurchaseLists(),
            # OrderPurchaseBuckets()
        ]


        pipeline = setup_tasks + read_files + create_output_buckets + insert_purchase_data + dedupe_purchase_lists + sort_data

        return pipeline

    @staticmethod
    def output():
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))


if __name__ == "__main__":
    luigi.run()