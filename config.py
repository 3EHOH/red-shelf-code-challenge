import datetime 
import os
import luigi

class ModelConfig(luigi.Config):
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

class NormanConfig(luigi.Config):
    norman_count=luigi.IntParameter()
    norman_chunksize=luigi.IntParameter()
    norman_stopafter=luigi.IntParameter()

class ConnieConfig(luigi.Config):
    connie_count=luigi.IntParameter()
    connie_chunksize=luigi.IntParameter()
    connie_stopafter=luigi.IntParameter()

class MySQLDBConfig(luigi.Config):
    prd_schema=luigi.Parameter()
    template_schema=luigi.Parameter()
    epb_schema=luigi.Parameter()

class MongoDBConfig(luigi.Config):
    md1_schema=luigi.Parameter()

#some default params
TARGET_PATH=os.path.join(os.path.dirname(__file__),'target/{feature}{rundate}'.format(
    feature=ModelConfig().jobname,
    rundate=ModelConfig().rundate)
)

#config classes
class PathConfig(luigi.Config):
    target_path=luigi.Parameter(default=TARGET_PATH)
