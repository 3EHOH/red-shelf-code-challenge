import luigi
import os
from config import PathConfig, PurchaseConfig
import json
from steps.purchaselistsorderer import PurchaseListsOrderer
from steps.filereader import FileReader

STEP = 'finalloader'


# A simple class to handle the last step of our ETL pipeline. It doesn't do any transformations; it simply takes the file
# generated from the last transformation step and loads it.

class FinalLoader(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def requires():
        return [PurchaseListsOrderer()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile + ".json"))

    def run(self):

        final_data = FileReader.read_file(PurchaseListsOrderer().datafile)


        with self.output().open('w') as out_file:
            out_file.write(json.dumps(final_data))


if __name__ == "__main__":
    luigi.run()
