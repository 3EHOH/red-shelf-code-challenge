import luigi
import os
from config import PathConfig, PurchaseConfig, BucketConfig
from pathlib import Path

STEP = 'preflightcheck'


class PreflightCheck(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        purchase_data_file = PurchaseConfig().purchase_data
        does_purchase_data_file_exist = self.file_exists_check(purchase_data_file)

        purchase_buckets_file = BucketConfig().purchase_buckets
        does_purchase_buckets_file_exist = self.file_exists_check(purchase_buckets_file)

        if not does_purchase_data_file_exist:
            raise ValueError("Error: Unable to find purchase data file")
        elif not does_purchase_buckets_file_exist:
            raise ValueError("Error: Unable to find purchase buckets file")
        else:
            self.output().open('w').close()

    @staticmethod
    def file_exists_check(file_name):
        if Path(file_name).is_file():
            return True


if __name__ == "__main__":
    luigi.run([
        'PreflightCheck',
        '--workers', '1',
        '--local-scheduler'])
