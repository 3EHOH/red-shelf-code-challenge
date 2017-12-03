import luigi
from config import PathConfig, BucketConfig
from steps.readdata import ReadData

STEP = 'readbucketdata'


class ReadBucketData(ReadData):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        # self.task_id = ReadData.task_id
        # super(ReadBucketData, self).__init__('purchase_buckets.csv', BucketConfig().bucket_keys.split(","))

        ReadData.__init__(self, BucketConfig().purchase_buckets, BucketConfig().bucket_keys.split(","))
        self.csv_file = BucketConfig().purchase_buckets
        self.csv_file_keys = BucketConfig().bucket_keys.split(",")


if __name__ == "__main__":
    luigi.run()