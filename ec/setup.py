import os
import luigi

from luigi.contrib.external_program import ExternalProgramTask

from ..config import ModelConfig, PathConfig
from ..run_55 import Run55

STEP = 'setup'

JARGS = 'java -d64 -Xms16G -Xmx80G -cp {cpath} -Dlog4j.configuration=file:/ecrfiles/scripts/log4j.properties control.BigKahuna typeinput={typeinput} jobname={jobname} clientID={clientID} jobstep=setup client={client} runname={runname} rundate={rundate} mapname={mapname} configfolder={configfolder} env={env} typeoutput=sql outputPath={outputPath} studybegin={studybegin} studyend={studyend} metadata={metadata} claim_file1={claim_file1} claim_rx_file1={claim_rx_file1} provider_file1={provider_file1} member_file1={member_file1} enroll_file1={enroll_file1}'.format(
    cpath=Run55.cpath(),
    typeinput=ModelConfig().typeinput,
    jobname=ModelConfig().jobname,
    clientID=ModelConfig().clientID,
    jobstep=STEP,
    client=ModelConfig().client,
    runname=ModelConfig().runname,
    rundate=ModelConfig().rundate,
    mapname=ModelConfig().mapname,
    configfolder=ModelConfig().configfolder,
    env=ModelConfig().env,
    outputPath=ModelConfig().outputPath,
    studybegin=ModelConfig().studybegin,
    studyend=ModelConfig().studyend,
    metadata=ModelConfig().metadata,
    claim_file1=ModelConfig().claim_file1,
    claim_rx_file1=ModelConfig().claim_rx_file1,
    provider_file1=ModelConfig().provider_file1,
    member_file1=ModelConfig().member_file1,
    enroll_file1=ModelConfig().enroll_file1)


class Setup(ExternalProgramTask):
    """ task to set up a new run """

    datafile = luigi.Parameter(default=STEP)

    def program_args(self):
        return JARGS.split(' ')

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,
                                              self.datafile))
    
    def run(self):
        super(Setup, self).run()
        self.output().open('w').close()


if __name__ == "__main__":
    luigi.run([
        'Setup', 
        '--workers', '1', 
        '--local-scheduler'])
