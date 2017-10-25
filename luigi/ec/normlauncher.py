import os
import sys
import luigi

from config import ModelConfig, PathConfig
from ec.postmap import PostMap

STEP = 'normlauncher'

class NormLauncher(luigi.Task):
    """ generate the post map report """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    def run(self):
        # super(PostMapReport, self).run()

        self.output().open('w').close()

if __name__ == "__main__":
    luigi.run([
        'NormLauncher',
        '--workers', '1',
        '--local-scheduler'])
