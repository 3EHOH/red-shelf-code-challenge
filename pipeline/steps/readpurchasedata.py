import luigi
from config import PathConfig, PurchaseConfig
from steps.readkeylesscsv import ReadKeylessCSV

STEP = 'readpurchasedata'


class ReadPurchaseData(ReadKeylessCSV):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        ReadKeylessCSV.__init__(self, PurchaseConfig().purchase_data, PurchaseConfig().purchase_keys.split(","))


if __name__ == "__main__":
    luigi.run()