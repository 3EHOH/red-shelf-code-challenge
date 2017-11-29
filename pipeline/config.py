import os
import luigi
import datetime

TARGET_PATH = os.path.join(os.path.dirname(__file__),'target/{rundate}'.format(
    rundate=datetime.datetime.now()))


class PathConfig(luigi.Config):
    target_path = luigi.Parameter(default=TARGET_PATH)
    purchase_buckets = luigi.Parameter()
    purchase_data = luigi.Parameter()
