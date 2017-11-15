import boto3
import sys
import luigi
import os
from luigi.contrib.external_program import ExternalProgramTask
from config import PathConfig, MySQLDBConfig
from luigi.contrib.external_program import ExternalProgramTask
from ec.post.optional.pac_super_groups import PACSuperGroups 
import requests

STEP = 'terminate'

class Terminate(ExternalProgramTask):

    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()
    
    def requires(self):
        return [PACSuperGroups(jobuid=self.jobuid)]

    def program_args(self):
        return 1
   
    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):
        self.shut_down_ec2()
        self.output().open('w').close()


    @staticmethod
    def shut_down_ec2():
        ec2 = boto3.client('ec2');
        #get instance id value and store it to val 
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        val = response.text
        #unconditional terminate
        ec2.stop_instances(InstanceIds=[val])
    

if __name__ == "__main__":
    luigi.run([
        'Terminate',
        '--workers', '1',
        '--local-scheduler'])


