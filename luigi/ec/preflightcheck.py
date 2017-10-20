import boto3
from config import PathConfig, MySQLDBConfig
import luigi
import mysql.connector
from mysql.connector import errorcode
import os
import pymongo
import psutil

STEP = 'preflightcheck'
MONGO_TIMEOUT_MS = 5000


class PreflightCheck(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        is_sftp_running = self.check_sftp_status()

        process_names = [proc.name() for proc in psutil.process_iter()]

        is_mongo_process_running = self.check_mongo_process_status(process_names)
        is_mysql_process_running = self.check_mysql_process_status(process_names)
        is_mysql_connection_established = self.check_mysql_connectivity()
        is_mongo_connection_established = self.check_mongo_connectivity()

        if not (is_sftp_running and is_mongo_process_running and is_mysql_process_running
                and is_mongo_connection_established and is_mysql_connection_established):
            raise ValueError("Error: Unable to connect to one or more process")
        else:
            self.output().open('w').close()

    @staticmethod
    def check_sftp_status():
        ec2 = boto3.resource('ec2')
        
        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                                                  {'Name': 'tag:Name', 'Values': ['SFTP Server']}])

        is_sftp_running = True if len(list(instances.all())) > 0 else False

        if not is_sftp_running:
            print("ERROR: Unable to reach a running SFTP Server")

        return is_sftp_running

    @staticmethod
    def check_mysql_connectivity():
        try:
            connection = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                                 user=MySQLDBConfig().prd_user,
                                                 passwd=MySQLDBConfig().prd_pass,
                                                 db=MySQLDBConfig().prd_schema)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return False
        else:
            connection.close()
            return True

    @staticmethod
    def check_mongo_connectivity():

        #TODO ultimately remove localhost default option once we are totally migrated to shared mode
        mongo_ip = os.getenv('MONGO_IP', 'localhost')

        try:
            client = pymongo.MongoClient("mongodb://" + mongo_ip + ":27017", serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)

        except pymongo.errors.ServerSelectionTimeoutError as err:
            print(err)
            return False
        else:
            client.close()
            return True


if __name__ == "__main__":
    luigi.run([
        'PreflightCheck',
        '--workers', '1',
        '--local-scheduler'])
