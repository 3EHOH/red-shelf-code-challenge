import luigi
import os
from config import PathConfig, PurchaseConfig
import csv
import json
from steps.preflightcheck import PreflightCheck

STEP = 'readpurchasedata'


class ReadPurchaseData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def requires():
        return [PreflightCheck()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        purchase_data_file = PurchaseConfig().purchase_data
        purchase_keys = PurchaseConfig().purchase_keys()
        purchase_data = []

        with open(purchase_data_file) as csvfile:
            purchase_reader = csv.reader(csvfile)
            for row in purchase_reader:
                purchase_record = {}
                for i in range(len(purchase_keys)):
                    purchase_record[purchase_keys[i]] = row[i]
                purchase_data.append(purchase_record)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(purchase_data))


if __name__ == "__main__":
    luigi.run([
        'ReadPurchaseData',
        '--workers', '1',
        '--local-scheduler'])