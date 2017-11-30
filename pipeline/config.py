import os
import luigi
import datetime

TARGET_PATH = os.path.join(os.path.dirname(__file__),'target/{rundate}'.format(
    rundate=datetime.datetime.now()))


class PathConfig(luigi.Config):
    target_path = luigi.Parameter(default=TARGET_PATH)


class PurchaseConfig(luigi.Config):
    purchase_data = luigi.Parameter()
    purchase_keys = luigi.Parameter()


class BucketConfig(luigi.Config):
    purchase_buckets = luigi.Parameter()
    bucket_keys = luigi.Parameter()
