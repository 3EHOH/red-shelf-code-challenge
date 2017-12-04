import luigi
from config import PathConfig, PurchaseConfig
from steps.csvkeyer import CsvKeyer

STEP = 'purchasedatareader'


# Pair the purchase data CSV file with its keys.


class PurchaseDataReader(CsvKeyer):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        CsvKeyer.__init__(self, PurchaseConfig().purchase_data, PurchaseConfig().purchase_keys.split(","))


if __name__ == "__main__":
    luigi.run()