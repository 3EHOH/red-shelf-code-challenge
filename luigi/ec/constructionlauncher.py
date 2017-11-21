import boto3
import luigi
import os
import time

from config import PathConfig, ModelConfig, MySQLDBConfig,  ConnieConfig
from ec.postnormalizationreport import PostNormalizationReport
from run_55 import Run55

STEP = 'constructionlauncher'
ec2 = boto3.resource('ec2')


class ConstructionLauncher(luigi.Task):
    """ launch external episode constructor services """
    datafile = luigi.Parameter(default=STEP)
    jobuid = luigi.IntParameter()

    def requires(self):
        return [PostNormalizationReport(jobuid=self.jobuid)]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))

    @staticmethod
    def format_security_groups():

        root_security_groups = os.getenv('ROOT_SECURITY_GROUPS')
        mysql_security_groups = os.getenv('MYSQL_SECURITY_GROUPS')

        all_security_groups = root_security_groups + " " + mysql_security_groups

        security_groups_formatted = []

        for security_group in all_security_groups.split():
            security_groups_formatted.append(ec2.SecurityGroup(security_group).group_name)

        return list(set(security_groups_formatted))

    @staticmethod
    def create_user_data_script():
        mysql_host = os.getenv('MYSQL_HOST')
        luigi_dir = os.getenv('LUIGI_DIR')

        user_data_host_info = """#!/bin/bash
        sed -i "s/prd.host=.*/prd.host={prdhost}/" {luigidir}/database.properties
        sed -i "s/prd.user=.*/prd.user={mysqluser}/" {luigidir}/database.properties
        sed -i "s/prd.password=.*/prd.password={mysqlpass}/" {luigidir}/database.properties
        sed -i "s/ecr.host=.*/ecr.host={ecrhost}/" {luigidir}/database.properties
        sed -i "s/ecr.user=.*/ecr.user={mysqluser}/" {luigidir}/database.properties
        sed -i "s/ecr.password=.*/ecr.password={mysqlpass}/" {luigidir}/database.properties
        sed -i "s/template.host=.*/template.host={templatehost}/" {luigidir}/database.properties
        sed -i "s/template.user=.*/template.user={mysqluser}/" {luigidir}/database.properties
        sed -i "s/template.password=.*/template.password={mysqlpass}/" {luigidir}/database.properties"""

        user_data_construction_command = ""

        for _ in range(ConnieConfig().processes_per_instance):
            user_data_construction_command = user_data_construction_command + '\n' + \
                                     "cd {luigidir}/; sleep 200; java -d64 -Xms8G -Xmx100G -cp {cpath} " \
                                     "-Dlog4j.configuration=file:/ecrfiles/scripts/log4jConnie.properties " \
                                     "control.ConstructionDriver configfolder={configfolder} " \
                                     "chunksize={chunksize} stopafter={stopafter} &"

        user_data_script = user_data_host_info + user_data_construction_command

        return user_data_script.format(
            luigidir=luigi_dir,
            prdhost=mysql_host,
            ecrhost=mysql_host,
            templatehost=mysql_host,
            epbhost=mysql_host,
            mysqluser=MySQLDBConfig().prd_user,
            mysqlpass=MySQLDBConfig().prd_pass,
            cpath=Run55.cpath(),
            configfolder=ModelConfig().configfolder,
            chunksize=ConnieConfig().chunksize,
            stopafter=ConnieConfig().stopafter)

    @staticmethod
    def name_constructors(construction_instances):

        construction_names = []

        host_name = os.getenv('HOSTNAME')

        for instance in construction_instances:
            tag_name = host_name + 'Connie_' + instance.id
            ec2.create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': tag_name}])
            construction_names.append(tag_name)

        return construction_names

    def run(self):

        #TODO add the sleeps into the command so the services dont get overwhelmed
        #sleep(600*self.conn_id)

        constructor_ami_id = os.getenv('CONNIE_AMI_ID')
        constructor_instance_type = os.getenv('CONNIE_INSTANCE_TYPE')
        key_name = os.getenv('KEY_PAIR')

        security_groups_formatted = self.format_security_groups()
        user_data_script = self.create_user_data_script()

        print("USER DATA SCRIPT CONNIE LAUNCHER: ", user_data_script) #sanity check

        construction_instances = ec2.create_instances(
            MinCount=1,
            MaxCount=ConnieConfig().instance_count,
            ImageId=constructor_ami_id,
            InstanceType=constructor_instance_type,
            KeyName=key_name,
            SecurityGroups=security_groups_formatted,
            UserData=user_data_script
        )

        time.sleep(30) #time buffer before iterating over and adding name tags

        constructor_names = self.name_constructors(construction_instances)

        time.sleep(60) #buffer to let instances reach running state before ending this step

        running_instance_count = len(list(ec2.instances.filter(Filters=[{'Name': 'instance-state-name',
                                                                         'Values': ['running']},
                                                                        {'Name': 'tag:Name',
                                                                         'Values': constructor_names}])))

        if not running_instance_count == ConnieConfig().instance_count:
            ValueError("Error: Construction instances not all running. Shutting down.")
        else:
            self.output().open('w').close()


if __name__ == "__main__":
    luigi.run([
        'ConstructionLauncher',
        '--workers', '1',
        '--local-scheduler'])
