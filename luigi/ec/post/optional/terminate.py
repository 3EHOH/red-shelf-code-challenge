import boto3
import sys
import luigi
import mysql.connector
import os
from luigi.contrib.external_program import ExternalProgramTask
import time
from config import PathConfig, MySQLDBConfig
import subprocess
from utils import update_status
from luigi.contrib.external_program import ExternalProgramTask
from ec.post.optional.pac_super_groups import PACSuperGroups 

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


    @staticmethod
    def shut_down_ec2():
        ec2 = boto3.client('ec2');
    
        subprocess.check_output(['wget','http://169.254.169.254/latest/meta-data/instance-id'])
    
        f = open("/home/ec2-user/instance-id","r")
        val= f.read()
        f.close();

        sql0 = "use ecr;"
        update_status(sql0)

        sql1 = "drop view if exists ecrjobview;"
        update_status(sql1)
 
        sql2 = "create view ecrjobview as select j.client_id, j.jobName , j.uid as jobUid, s.uid, s.sequence, s.stepName, s.description, s.status, s.updated from processJob j join processJobStep s on s.jobUid = j.uid and j.client_id = 'Test' order by j.uid, s.sequence asc;"
        update_status(sql2)

        sql3 = "select exists(select * from ecrjobview where stepName='pacanalysis' and status='Complete');"

        if sql3 == True:
            ec2.stop_instances(InstanceIds=[val])
        else:
            ec2.stop_instances(InstanceIds=[val])

if __name__ == "__main__":
    luigi.run([
        'Terminate',
        '--workers', '1',
        '--local-scheduler'])


