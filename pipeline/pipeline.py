import os
import sys
import luigi

class PipelineTask(luigi.WrapperTask):

    def requires(self):

        setup_tasks = [
            PreflightCheck()
        ]



        return tasks

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))

if __name__ == "__main__":
    luigi.run([
        'PipelineTask',
        '--workers', sys.argv[1],
        '--local-scheduler'])