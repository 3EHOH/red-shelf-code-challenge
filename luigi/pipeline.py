import os
import sys
import luigi
from config import ConnieConfig, ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from ec.setup import Setup
from ec.analyze import Analyze
from ec.preflightcheck import PreflightCheck
from ec.schemacreate import SchemaCreate
from ec.map import Map
from ec.postmap import PostMap
from ec.postmapreport import PostMapReport
from ec.normtracker import NormTracker
from ec.normlauncher import NormLauncher
from ec.normalization import Normalize
from ec.postnormalization import PostNormalize
from ec.postnormalizationreport import PostNormalizationReport
from ec.construction import Construct
from ec.postconstructionreport import PostConstructionReport
from ec.post.epidedupe import Dedupe
from ec.post.providerattribution import ProviderAttribution
from ec.post.costrollups import CostRollUps
from ec.post.filteredcostrollups import FilteredCostRollUps
from ec.post.masterunfiltered_ra_sa import MasterUnfilteredRASA
from ec.post.rasamodel import RASAModel
from ec.post.red import RED
from ec.post.res import RES
from ec.post.savingsummary import SavingSummary
from ec.post.coreservices import CoreServices
from ec.post.pacanalysis import PACAnalysis
from ec.post.pasanalysis import PASAnalysis
from ec.post.maternity import Maternity
from ec.post.optional.epipacs import EpiPACs
from ec.post.optional.rainputfiles import RAInputFiles
from ec.post.optional.pac_rate_ra_program import PACRateRAProgram
from ec.post.optional.provider_pac_rates import ProviderPACRates
from ec.post.optional.hci3_reliability_analysis import HCI3ReliabilityAnalysis
from ec.post.optional.ieva import IEVA
from ec.post.optional.pac_super_groups import PACSuperGroups
from ec.post.optional.terminate import Terminate

class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = ModelConfig().rundate 
    jobuid = ModelConfig().jobuid 

    def requires(self):
        # basic setup tasks
        setup_tasks = [
            PreflightCheck(),
            Setup(),
            Analyze(jobuid=self.jobuid),
            SchemaCreate(jobuid=self.jobuid)
        ]
        # mapping tasks
        map_tasks = [
                Map(jobuid=self.jobuid),
                PostMap(jobuid=self.jobuid),
                PostMapReport(jobuid=self.jobuid)
        ] 

        norm_tasks = [
            NormLauncher(jobuid=self.jobuid),
            NormTracker(jobuid=self.jobuid),
            PostNormalize(jobuid=self.jobuid),
            PostNormalizationReport(jobuid=self.jobuid)
        ]

        # construction tasks
        conn_ids = list(range(0, ConnieConfig().count))
        conn_tasks = [Construct(jobuid=self.jobuid, conn_id=id) for id in conn_ids]
        conn_tasks.append(PostConstructionReport(jobuid=self.jobuid))

        # post EC tasks
        postec_tasks = [
            Dedupe(jobuid=self.jobuid),
            ProviderAttribution(jobuid=self.jobuid),
            CostRollUps(jobuid=self.jobuid),
            FilteredCostRollUps(jobuid=self.jobuid),
            MasterUnfilteredRASA(jobuid=self.jobuid),
            RASAModel(jobuid=self.jobuid),
            RED(jobuid=self.jobuid),
            RES(jobuid=self.jobuid)
        ]
        
        # optional post EC tasks
        opt_postec_tasks = [
            SavingSummary(jobuid=self.jobuid),
            CoreServices(jobuid=self.jobuid),
            PACAnalysis(jobuid=self.jobuid),
            PASAnalysis(jobuid=self.jobuid),
            Maternity(jobuid=self.jobuid)
        ]

        # RSPR tasks
        rspr_tasks = [
            EpiPACs(jobuid=self.jobuid),
            RAInputFiles(jobuid=self.jobuid),
            PACRateRAProgram(jobuid=self.jobuid),
            ProviderPACRates(jobuid=self.jobuid),
            HCI3ReliabilityAnalysis(jobuid=self.jobuid)
        ]

        # IEVA and PAC Super Groups
        ieva_pacsg_tasks = [
            IEVA(jobuid=self.jobuid),
            PACSuperGroups(jobuid=self.jobuid)
        ]
        #shutdown tasks
        shutdown_task = [
            Terminate(jobuid=self.jobuid)
        ]

        # Cleanup tasks
        # cleanup_tasks = [
        #     CollectOutput(jobuid=self.jobuid),
        #     UploadOutput(jobuid=self.jobuid),
        #     CollectLogs(jobuid=self.jobuid),
        #     UploadLogs(jobuid=self.jobuid)
        # ]

        # Let's go!
        tasks = setup_tasks + map_tasks + norm_tasks + conn_tasks + \
                postec_tasks + opt_postec_tasks + rspr_tasks + ieva_pacsg_tasks + shutdown_task
        return tasks

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write("successly ran pipeline on {}".format(self.date))

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path,"dummy"))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('pipeline.py <workers>')
        exit(-1)
    luigi.run([
        'PipelineTask', 
        '--workers', sys.argv[1],
        '--local-scheduler'])
