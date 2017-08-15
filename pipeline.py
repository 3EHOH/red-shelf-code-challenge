import os
import sys
import luigi
from config import ConnieConfig, ModelConfig, MySQLDBConfig, NormanConfig, PathConfig
from setup import Setup
from analyze import Analyze
from schemacreate import SchemaCreate
from map import Map
from postmap import PostMap
from postmapreport import PostMapReport
from normalization import Normalize
from postnormalization import PostNormalize
from postnormalizationreport import PostNormalizationReport
from construction import Construct
from postconstructionreport import PostConstructionReport
from epidedupe import Dedupe
from providerattribution import ProviderAttribution
from costrollups import CostRollUps
from filteredcostrollups import FilteredCostRollUps
from masterunfiltered_ra_sa import MasterUnfilteredRASA
from rasamodel import RASAModel
from red import RED
from res import RES
from savingsummary import SavingSummary
from coreservices import CoreServices
from pacanalysis import PACAnalysis
from pasanalysis import PASAnalysis
from maternity import Maternity
from epipacs import EpiPACs
from rainputfiles import RAInputFiles
from pac_rate_ra_program import PACRateRAProgram
from provider_pac_rates import ProviderPACRates
from hci3_reliability_analysis import HCI3ReliabilityAnalysis
from ieva import IEVA
from pac_super_groups import PACSuperGroups

class PipelineTask(luigi.WrapperTask):
    """Wrap up all the tasks for the pipeline into a single task
    So we can run this pipeline by calling this dummy task"""
    date = ModelConfig().rundate 
    jobuid = ModelConfig().jobuid 

    def requires(self):
        # basic setup tasks
        setup_tasks = [
            Setup(),
            Analyze(jobuid=self.jobuid),
            SchemaCreate(jobuid=self.jobuid)
        ]
        # mapping tasks
        map_tasks = [
                Map(jobuid=self.jobuid),
                PostMap(jobuid=self.jobuid)
                #PostMapReport(jobuid=self.jobuid)
        ] 
        # normalization tasks
        norm_ids = list(range(0, NormanConfig().count))
        norm_tasks = [Normalize(jobuid=self.jobuid, norm_id=id) for id in norm_ids]
        norm_tasks.append(PostNormalize(jobuid=self.jobuid))
        norm_tasks.append(PostNormalizationReport(jobuid=self.jobuid))

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

        # Let's go!
        tasks = setup_tasks + map_tasks + norm_tasks + conn_tasks + \
                postec_tasks + opt_postec_tasks + rspr_tasks + ieva_pacsg_tasks
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
