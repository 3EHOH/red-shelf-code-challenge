import boto3
import os
import luigi
import sys
import time

from config import PathConfig, NormanConfig
from ec.postmap import PostMap

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

        # norm_image_id = os.getenv('NORM_SERVICE_IMAGE_ID')
        # norm_instance_type = os.getenv('NORM_SERVICE_INSTANCE_TYPE')

        norm_n_instances = NormanConfig().n_instances
        norm_n_processes = NormanConfig().n_processes_per_instance

        # norm_chunk_size = NormanConfig().chunksize
        # norm_stopafter = NormanConfig().stopafter

        # norm_server_key_name = os.getenv('NORM_SERVICE_KEY_NAME')
        # norm_server_security_groups = os.getenv('NORM_SERVICE_SECURITY_GROUPS')

        ec2 = boto3.resource('ec2')

        user_data_script = 'java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}'.format(
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=NormanConfig().chunksize,
            stopafter=NormanConfig().stopafter)'

        norm_instances = ec2.create_instances(
            MinCount=1,
            MaxCount=norm_n_instances,
            ImageId='ami-1ac10762',  # replace with config or env var
            InstanceType='r3.8xlarge',  # replace with config or env var
            KeyName='PFS',  # replace with config or env var
            SecurityGroups=['PFS'], # replace with config or env var
            UserData=user_data_script,
            NoAssociatePublicIpAddress="False"
        )

        norm_names = []

        for instance in norm_instances:
            tag_name = 'Norm_' + instance.id
            ec2.create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': tag_name}])
            norm_names.append(tag_name)

        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])

        n_tries = 0

        while (not len(list(instances)) == norm_n_instances) or n_tries < 3:
            time.sleep(60)
            instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])
            n_tries += 1
            if n_tries == 3:
                raise ValueError("Error: Norm instances not all running after multiple checks")

        self.output().open('w').close()

if __name__ == "__main__":
    luigi.run([
        'NormLauncher',
        '--workers', '1',
        '--local-scheduler'])
