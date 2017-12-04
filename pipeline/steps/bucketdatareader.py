import luigi
from config import PathConfig, BucketConfig
from steps.csvkeyer import CsvKeyer

STEP = 'bucketdatareader'

# Pair the bucket data CSV file with its keys.

class BucketDataReader(CsvKeyer):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        CsvKeyer.__init__(self, BucketConfig().purchase_buckets, BucketConfig().bucket_keys.split(","))


if __name__ == "__main__":
    luigi.run()