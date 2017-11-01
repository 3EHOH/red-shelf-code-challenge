import boto3
import luigi
import os
import socket
import time

from config import PathConfig, ModelConfig, MySQLDBConfig,  NormanConfig, MongoDBConfig
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

        ec2 = boto3.resource('ec2')

        user_data_host_info = """#!/bin/bash
        sed -i "s/md1.host=.*/md1.host={mongohost}/" {luigidir}/database.properties
        sed -i "s/prd.host=.*/prd.host={prdhost}/" {luigidir}/database.properties
        sed -i "s/ecr.host=.*/ecr.host={ecrhost}/" {luigidir}/database.properties
        sed -i "s/template.host=.*/template.host={templatehost}/" {luigidir}/database.properties"""

        user_data_norm_command = ""

        for _ in range(NormanConfig().processes_per_instance):
            user_data_norm_command = user_data_norm_command + '\n' + "cd {luigidir}/; java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}"

        user_data_script = user_data_host_info + user_data_norm_command

        user_data_script_populated = user_data_script.format(
            mongohost=os.getenv('MONGO_IP'),
            luigidir=os.getenv('LUIGI_DIR'),
            prdhost=MySQLDBConfig().prd_host,
            ecrhost=MySQLDBConfig().prd_host,  #there is no ecr host var in MySQLDBConfig
            templatehost=MySQLDBConfig().template_host,
            epbhost=MySQLDBConfig().epb_host,
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=NormanConfig().chunksize,
            stopafter=NormanConfig().stopafter)

        security_groups = os.getenv('SECURITY_GROUPS')

        security_groups_formatted = []

        for security_group in security_groups.split():
            security_groups_formatted.append(ec2.SecurityGroup(security_group).group_name)

        norm_ami_id = os.getenv('NORMAN_AMI_ID')
        norm_instance_type = os.getenv('NORMAN_INSTANCE_TYPE')
        key_name = os.getenv('KEY_NAME')

        print("SCRIPT: ", user_data_script_populated)

        norm_instances = ec2.create_instances(
            MinCount=1,
            MaxCount=NormanConfig().instance_count,
            ImageId=norm_ami_id,
            InstanceType=norm_instance_type,
            KeyName=key_name,
            SecurityGroups=security_groups_formatted,
            UserData=user_data_script_populated
        )

        print("NORM INSTANCES", norm_instances)

        time.sleep(30) #time buffer before iterating over and adding name tags

        norm_names = []

        for instance in norm_instances:
            tag_name = 'Norm_' + instance.id
            ec2.create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': tag_name}])
            norm_names.append(tag_name)

        time.sleep(60) #buffer to let instances reach running state before ending this step

        running_instance_count = len(list(ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])))

        if not running_instance_count == NormanConfig().instance_count:
            ValueError("Error: Norm instances not all running after multiple checks")
        else:
            self.output().open('w').close()

if __name__ == "__main__":
    luigi.run([
        'NormLauncher',
        '--workers', '1',
        '--local-scheduler'])
