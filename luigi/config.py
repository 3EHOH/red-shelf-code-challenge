import datetime 
import os
import luigi

class ModelConfig(luigi.Config):
    jobuid=luigi.IntParameter()
    typeinput=luigi.Parameter()
    jobname=luigi.Parameter()
    clientID=luigi.Parameter()
    jobstep=luigi.Parameter()
    client=luigi.Parameter()
    runname=luigi.Parameter()
    rundate=luigi.Parameter()
    mapname=luigi.Parameter()
    configfolder=luigi.Parameter()
    env=luigi.Parameter()
    typeoutput=luigi.Parameter()
    outputPath=luigi.Parameter()
    studybegin=luigi.Parameter()
    studyend=luigi.Parameter()
    metadata=luigi.Parameter()
    claim_file1=luigi.Parameter()
    claim_rx_file1=luigi.Parameter()
    provider_file1=luigi.Parameter()
    member_file1=luigi.Parameter()
    enroll_file1=luigi.Parameter()


class SlackChannelName(luigi.Config):
    #User defined channel name
    channel_name=luigi.Parameter()

class NormanConfig(luigi.Config):
    chunksize=luigi.IntParameter()
    stopafter=luigi.IntParameter()
    processes_per_instance=luigi.IntParameter()
    instance_count=luigi.IntParameter()

class ConnieConfig(luigi.Config):
    count=luigi.IntParameter()
    chunksize=luigi.IntParameter()
    stopafter=luigi.IntParameter()

class MySQLDBConfig(luigi.Config):
    # job database
    prd_host=luigi.Parameter()
    prd_port=luigi.IntParameter()
    prd_schema=luigi.Parameter()
    prd_user=luigi.Parameter()
    prd_pass=luigi.Parameter()
    # output template
    template_host=luigi.Parameter()
    template_port=luigi.IntParameter()
    template_schema=luigi.Parameter()
    template_user=luigi.Parameter()
    template_pass=luigi.Parameter()
    # episode builder
    epb_host=luigi.Parameter()
    epb_port=luigi.IntParameter()
    epb_schema=luigi.Parameter()
    epb_user=luigi.Parameter()
    epb_pass=luigi.Parameter()

class MongoDBConfig(luigi.Config):
    md1_schema=luigi.Parameter()

class AwsConfig(luigi.Config):
    job_id=luigi.Parameter()
    sftp_server=luigi.Parameter()
    key_pair_name=luigi.Parameter()
    file_name=luigi.Parameter()


#some default params
TARGET_PATH=os.path.join(os.path.dirname(__file__),'target/{feature}{rundate}'.format(
    feature=ModelConfig().jobname,
    rundate=ModelConfig().rundate)
)

#config classes
class PathConfig(luigi.Config):
    target_path=luigi.Parameter(default=TARGET_PATH)
    postec_path=luigi.Parameter(default='/ecrfiles/post_ec')
