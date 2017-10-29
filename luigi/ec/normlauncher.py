import boto3
import os
import luigi
import sys
import time

from config import PathConfig, ModelConfig, NormanConfig
from ec.postmap import PostMap
from run_55 import Run55

STEP = 'normlauncher'

class NormLauncher(luigi.Task):
    """ launch external normalization services """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [PostMap(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    def run(self):

        # MAYBE run the hack here?
        # if self.norm_id == 0:
        #     sql = "update processJobStep set status = 'Active', stepStart = now() where jobUid = {jobuid} and stepName = 'normalization';".format(jobuid=self.jobuid)
        #     update_status(sql)

        # norm_image_id = os.getenv('NORM_SERVICE_IMAGE_ID')
        # norm_instance_type = os.getenv('NORM_SERVICE_INSTANCE_TYPE')

        norm_n_instances = NormanConfig().count
        #norm_n_processes = NormanConfig().n_processes_per_instance

        # norm_chunk_size = NormanConfig().chunksize
        # norm_stopafter = NormanConfig().stopafter

        # norm_server_key_name = os.getenv('NORM_SERVICE_KEY_NAME')
        # norm_server_security_groups = os.getenv('NORM_SERVICE_SECURITY_GROUPS')

        ec2 = boto3.resource('ec2')

        #todo pretty sure we don't need the luigi.cfg changes - remove later

        user_data_script = """#!/bin/bash
        sed -i "s/md1.host=.*/md1.host=dummyvalue/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/prd.host=.*/prd.host=dummyvalue/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/ecr.host=.*/ecr.host=dummyvalue/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/template.host=.*/template.host=dummyvalue/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/prd_host=.*/prd_host=dummyvalue/" /home/ec2-user/payformance/luigi/luigi.cfg
        sed -i "s/template_host=.*/template_host=dummyvalue/" /home/ec2-user/payformance/luigi/luigi.cfg
        sed -i "s/epb_host=.*/epb_host=dummyvalue/" /home/ec2-user/payformance/luigi/luigi.cfg
        """.format()

        # mongoip=os.getenv('MONGO_IP'),
        # luigidir=os.getenv('LUIGI_DIR')

        # user_data_script = """#!/bin/bash
        #     java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}""".format(
        #     cpath=Run55.cpath(),
        #     configfolder=ModelConfig().configfolder,
        #     chunksize=NormanConfig().chunksize,
        #     stopafter=NormanConfig().stopafter)

        print("SCRIPT: ", user_data_script)

        norm_instances = ec2.create_instances(
            MinCount=1,
            MaxCount=1,
            ImageId='ami-1ac10762',  # replace with config or env var
            InstanceType='r3.8xlarge',  # replace with config or env var
            KeyName='PFS',  # replace with config or env var
            SecurityGroups=['PFS'],  # replace with config or env var
            UserData=user_data_script
        )

        norm_names = []

        for instance in norm_instances:
            tag_name = 'Norm_' + instance.id
            ec2.create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': tag_name}])
            norm_names.append(tag_name)

        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])
        running_instance_count = len(list(instances))
        n_tries = 0

        time.sleep(100)

        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])

        # while (not len(list(instances)) == norm_n_instances) or n_tries < 3:
        #     if n_tries == 3:
        #         raise ValueError("Error: Norm instances not all running after multiple checks")
        #     time.sleep(60)

        if len(list(instances)) >= norm_n_instances:
            self.output().open('w').close()
        else:
            ValueError("Error: Norm instances not all running after multiple checks")

if __name__ == "__main__":
    luigi.run([
        'NormLauncher',
        '--workers', '1',
        '--local-scheduler'])
