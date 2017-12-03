import luigi
from config import PathConfig, PurchaseConfig
from steps.readdata import ReadData

STEP = 'readpurchasedata'


class ReadPurchaseData(ReadData):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        # self.task_id = ReadData.task_id
        # super(ReadBucketData, self).__init__('purchase_buckets.csv', BucketConfig().bucket_keys.split(","))

        ReadData.__init__(self, PurchaseConfig().purchase_data, PurchaseConfig().purchase_keys.split(","))
        self.csv_file = PurchaseConfig().purchase_data
        self.csv_file_keys = PurchaseConfig().purchase_keys.split(",")


if __name__ == "__main__":
    luigi.run()