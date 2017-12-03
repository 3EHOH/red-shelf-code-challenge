import luigi
from config import PathConfig, BucketConfig
from steps.readkeylesscsv import ReadKeylessCSV

STEP = 'readbucketdata'


class ReadBucketData(ReadKeylessCSV):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        ReadKeylessCSV.__init__(self, BucketConfig().purchase_buckets, BucketConfig().bucket_keys.split(","))


if __name__ == "__main__":
    luigi.run()