import boto3
import os
import luigi
import sys
import time
import socket

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

        print("ENVIRONMENT", os.environ)

        print("DO THE ENV VARS SHOW UP", os.environ['MONGO_IP'], os.environ['NORMAN_AMI_ID'])

        ec2 = boto3.resource('ec2')

        user_data_host_info = """#!/bin/bash
        sed -i "s/md1.host=.*/md1.host={mongohost}/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/prd.host=.*/prd.host={prdhost}/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/ecr.host=.*/ecr.host={ecrhost}/" /home/ec2-user/payformance/luigi/database.properties
        sed -i "s/template.host=.*/template.host={templatehost}/" /home/ec2-user/payformance/luigi/database.properties"""

        user_data_norm_command = ""

        for _ in range(NormanConfig().processes_per_instance):
            user_data_norm_command = user_data_norm_command + '\n' + "cd /home/ec2-user/payformance/luigi/; java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}"

        user_data_script = user_data_host_info + user_data_norm_command

        user_data_script_populated = user_data_script.format(
            mongohost=MongoDBConfig().mongo_ip, #os.getenv('MONGO_IP'),
            luigidir=os.getenv('LUIGI_DIR'), #this var isn't being used - it's hardcoded as /home/ec2-user/payformance/luigi/
            prdhost=MySQLDBConfig().prd_host,   #os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())), #until we have a separate mysql instance
            ecrhost=MySQLDBConfig().prd_host,  #there is no ecr host var in MySQLDBConfig
            templatehost=MySQLDBConfig().template_host,     #os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
            epbhost=MySQLDBConfig().epb_host, #os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=NormanConfig().chunksize,
            stopafter=NormanConfig().stopafter)

        # user_data = """#!/bin/bash
        # sed -i "s/md1.host=.*/md1.host={mongohost}/" /home/ec2-user/payformance/luigi/database.properties
        # sed -i "s/prd.host=.*/prd.host={prdhost}/" /home/ec2-user/payformance/luigi/database.properties
        # sed -i "s/ecr.host=.*/ecr.host={ecrhost}/" /home/ec2-user/payformance/luigi/database.properties
        # sed -i "s/template.host=.*/template.host={templatehost}/" /home/ec2-user/payformance/luigi/database.properties
        # cd /home/ec2-user/payformance/luigi/; java -d64 -Xms8G -Xmx48G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4jNorman.properties control.NormalizationDriver configfolder={configfolder} chunksize={chunksize} stopafter={stopafter}"""\
        #     .format(
        #     mongohost=os.getenv('MONGO_IP'),
        #     luigidir=os.getenv('LUIGI_DIR'),
        #     prdhost=os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
        #     ecrhost=os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
        #     templatehost=os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
        #     epbhost=os.getenv('ROOT_IP', socket.gethostbyname(socket.gethostname())),
        #     cpath=Run55.cpath(),
        #     configfolder=ModelConfig().configfolder,
        #     chunksize=NormanConfig().chunksize,
        #     stopafter=NormanConfig().stopafter)


        # user_data_script_formatted = user_data_script.format(
        #     mongohost=os.getenv('MONGO_IP'),
        #     luigidir=os.getenv('LUIGI_DIR'),
        #     prdhost=os.getenv('ROOT_IP'),
        #     ecrhost=os.getenv('ROOT_IP'),
        #     templatehost=os.getenv('ROOT_IP'),
        #     epbhost=os.getenv('ROOT_IP'),
        #     cpath=Run55.cpath(),
        #     configfolder=ModelConfig().configfolder,
        #     chunksize=NormanConfig().chunksize,
        #     stopafter=NormanConfig().stopafter)

        # security_groups = os.getenv('SECURITY_GROUPS')
        #
        # security_groups_formatted = []
        #
        # for security_group in security_groups.split():
        #     security_groups_formatted.append(ec2.SecurityGroup(security_group).group_name)

        norm_ami_id = NormanConfig().ami_id #os.getenv('NORMAN_AMI_ID')
        norm_instance_type = NormanConfig().instance_type  #os.getenv('NORMAN_INSTANCE_TYPE')
        key_name = ModelConfig().key_name
        # os.getenv('KEY_NAME')

        print("SCRIPT: ", user_data_script_populated)

        norm_instances = ec2.create_instances(
            MinCount=1,
            MaxCount=NormanConfig().instance_count,
            ImageId=norm_ami_id,
            InstanceType=norm_instance_type,
            KeyName=key_name,
            SecurityGroups=['launch-wizard-1', 'PFS', 'PFS_external_access'],  # replace with env var
            UserData=user_data_script_populated
        )

        print("NORM INSTANCES", norm_instances)

        time.sleep(60)

        norm_names = []


        for instance in norm_instances:
            tag_name = 'Norm_' + instance.id
            ec2.create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': tag_name}])
            norm_names.append(tag_name)
            print("LOOPING: ", instance.id)

        # instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])
        # running_instance_count = len(list(instances))
        # n_tries = 0

        time.sleep(100)

        # instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}])

        for _ in range(3):
            time.sleep(10)
            print("Number of running norms: ", len(list(ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': norm_names}]))))

        # while (not len(list(instances)) == norm_n_instances) or n_tries < 3:
        #     if n_tries == 3:
        #         raise ValueError("Error: Norm instances not all running after multiple checks")
        #     time.sleep(60)

        # if len(list(instances)) >= norm_n_instances:
            self.output().open('w').close()
        # else:
        #     ValueError("Error: Norm instances not all running after multiple checks")

if __name__ == "__main__":
    luigi.run([
        'NormLauncher',
        '--workers', '1',
        '--local-scheduler'])
