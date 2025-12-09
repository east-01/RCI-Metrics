from dataclasses import dataclass
import pandas as pd

from plugins.rci_plugins.rci_identifiers import GrafanaIdentifier, SummaryIdentifier
from src.data.data_repository import DataRepository
from src.data.filters import filter_type
from src.data.identifier import AnalysisIdentifier
from src.parameter_utils import ConfigurationException
from src.plugin_mgmt.plugins import Analysis, AnalysisDriverPlugin
from src.program_data import ProgramData
from src.utils.timeutils import get_range_printable

import plugins.rci_plugins.analyses.summary_driver as pkg

@dataclass(frozen=True)
class SummaryAnalysis(Analysis):
    """ A wrapper for the standard analysis so we can capture it with the SummaryDriver.
        The SummaryAnalysis is the higest level analysis for the monthly configuration, collecting
            cpu/gpu hours and jobs by namespace, total cpu/gpu hours and jobs. """
    pass

@dataclass(frozen=True)
class SummaryData():
    """ The SummaryData dataclass acts as a struct for the readable_period, summary_df, and the
            cpu/gpu_dfs. The str() method also compiles the data into a readable printout.
    """
    readable_period: str
    cpujobstotal: int
    gpujobstotal: int
    jobstotal: int
    cpuhourstotal: int
    gpuhourstotal: int
    usedcapacity: float
    cpu_df: pd.DataFrame
    gpu_df: pd.DataFrame
    cpu_jh_users_df: pd.DataFrame
    gpu_jh_users_df: pd.DataFrame

    def __str__(self):
                # {'\n  '.join(self.summary_df.to_string().split('\n'))}
        return f"""Summary for {self.readable_period}

                {(
                    'Top 5 CPU namespaces:\n    ' + '\n    '.join(self.cpu_df.to_string().split('\n'))
                    if len(self.cpu_df) > 0 else
                    'Top 5 CPU namespaces empty.'
                )}

                {(
                    'Top 5 GPU namespaces:\n    ' + '\n    '.join(self.gpu_df.to_string().split('\n'))
                    if len(self.gpu_df) > 0 else
                    'Top 5 GPU namespaces empty.'
                )}

                {(
                    'Top 5 CPU users:\n    ' + '\n    '.join(self.cpu_jh_users_df.to_string().split('\n'))
                    if len(self.cpu_jh_users_df) > 0 else
                    'Top 5 CPU users empty.'
                )}

                {(
                    'Top 5 GPU users:\n    ' + '\n    '.join(self.gpu_jh_users_df.to_string().split('\n'))
                    if len(self.gpu_jh_users_df) > 0 else
                    'Top 5 GPU users empty.'
                )}
                """

class SummaryDriver(AnalysisDriverPlugin):
    # pkg. is a workaround to point to the global definition of SummaryAnalysis instead of the file definition
    SERVED_TYPE=pkg.SummaryAnalysis

    def verify_config_section(self, config_section):
        # Optional config section, if its None that's fine
        if(config_section is None):
            return True
        
        if(set(config_section.keys()) != set(["top5hours_blacklist"])):
            raise ConfigurationException("The configuration section for SummaryDriver only expects a top5hours_blacklist section.")
        
        if(not isinstance(config_section["top5hours_blacklist"], list)):
            raise ConfigurationException(f"The configuation section for SummaryDriver top5hours_blacklist is expected to be a list. Is type: {type(config_section["top5hours_blacklist"])}")

        return True

    def run_analysis(self, analysis, prog_data: ProgramData, config_section: dict):
        data_repo: DataRepository = prog_data.data_repo
    
        identifiers = data_repo.filter_ids(filter_type(GrafanaIdentifier))
        for identifier in identifiers:
            start_ts = identifier.start_ts
            end_ts = identifier.end_ts

            summary_id = SummaryIdentifier(start_ts, end_ts)

            if(data_repo.contains(summary_id)):
                continue

            src_ids = get_src_ids(start_ts, end_ts)
            for src_id in src_ids:
                if(not data_repo.contains(src_id)):
                    raise ValueError(f"Can't summarize {get_range_printable(start_ts, end_ts)} the data_repo is missing expected identifier: {src_id}.")                
            
            summary_data = generate_analysis(data_repo, config_section, src_ids)
            data_repo.add(summary_id, summary_data)

def get_src_ids(start_ts: int, end_ts: int):
    # NOTE: For future reference this can be keyed, so instead of only picking monthly 
    #   identifiers we can add a key implementation and pick multiple
    cpu_src_id = GrafanaIdentifier(start_ts, end_ts, "cpu", "monthly")
    gpu_src_id = GrafanaIdentifier(start_ts, end_ts, "gpu", "monthly")
    cpu_jh_users_src_id = GrafanaIdentifier(start_ts, end_ts, "cpu", "jupyterhub")
    gpu_jh_users_src_id = GrafanaIdentifier(start_ts, end_ts, "gpu", "jupyterhub")
    usedcapacity_src_id = GrafanaIdentifier(start_ts, end_ts, "storage", "usedcapacity")

    return cpu_src_id, gpu_src_id, cpu_jh_users_src_id, gpu_jh_users_src_id, usedcapacity_src_id

def generate_analysis(data_repo: DataRepository, config_section: dict, src_ids: tuple) -> SummaryData:

    cpu_src_id, gpu_src_id, cpu_jh_users_src_id, gpu_jh_users_src_id, usedcapacity_src_id = src_ids

    # Collect analysis identifiers
    cpuhours = AnalysisIdentifier(cpu_src_id, "cpuhours")
    cpuhourstotal = AnalysisIdentifier(cpuhours, "cpuhourstotal")
    cpujobs = AnalysisIdentifier(cpu_src_id, "cpujobs")
    cpujobstotal = AnalysisIdentifier(cpujobs, "cpujobstotal")
    jobstotal = AnalysisIdentifier(cpujobstotal, "jobstotal")
    cpujhhours = AnalysisIdentifier(cpu_jh_users_src_id, "cpujhpodhours")

    gpuhours = AnalysisIdentifier(gpu_src_id, "gpuhours")
    gpuhourstotal = AnalysisIdentifier(gpuhours, "gpuhourstotal")
    gpujobs = AnalysisIdentifier(gpu_src_id, "gpujobs")
    gpujobstotal = AnalysisIdentifier(gpujobs, "gpujobstotal")
    gpujhhours = AnalysisIdentifier(gpu_jh_users_src_id, "gpujhpodhours")

    usedcapacity_id = AnalysisIdentifier(usedcapacity_src_id, "usedcapacity")

    # Shorthand for the get_data method call for readability
    gd = data_repo.get_data

    # Generate top 5 hours dataframes
    if("top5hours_blacklist" in config_section.keys()):
        top5_blacklist_lambda = lambda row: row.iloc[0] not in config_section['top5hours_blacklist']
    else:
        top5_blacklist_lambda = True    

    cpu_df = gd(cpuhours)
    cpu_df = cpu_df[cpu_df.apply(top5_blacklist_lambda, axis=1)].iloc[0:5].reset_index(drop=True)

    gpu_df = gd(gpuhours)
    gpu_df = gpu_df[gpu_df.apply(top5_blacklist_lambda, axis=1)].iloc[0:5].reset_index(drop=True)

    # Generate top 5 users dataframes
    cpu_jh_df = gd(cpujhhours)
    cpu_jh_df = cpu_jh_df.iloc[0:5].reset_index(drop=True)

    gpu_jh_df = gd(gpujhhours)
    gpu_jh_df = gpu_jh_df.iloc[0:5].reset_index(drop=True)

    readable_period = get_range_printable(cpu_src_id.start_ts, cpu_src_id.end_ts, 3600)
    return SummaryData(
        readable_period=readable_period,
        cpujobstotal=gd(cpujobstotal),
        gpujobstotal=gd(gpujobstotal),
        jobstotal=gd(jobstotal),
        cpuhourstotal=gd(cpuhourstotal),
        gpuhourstotal=gd(gpuhourstotal),
        usedcapacity=gd(usedcapacity_id),
        cpu_df=cpu_df,
        gpu_df=gpu_df,
        cpu_jh_users_df=cpu_jh_df,
        gpu_jh_users_df=gpu_jh_df
    )