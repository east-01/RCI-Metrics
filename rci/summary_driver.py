from dataclasses import dataclass
import pandas as pd

from src.plugin_mgmt.plugins import AnalysisDriverPlugin
from src.program_data.config import ConfigurationException
from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository
from src.data.identifier import AnalysisIdentifier
from plugins.rci.rci_identifiers import GrafanaIdentifier, SummaryIdentifier
from src.data.filters import filter_type
from src.utils.timeutils import get_range_printable
from src.plugin_mgmt.plugins import Analysis

import plugins.rci.summary_driver as pkg_driver

@dataclass(frozen=True)
class SummaryAnalysis(Analysis):
    pass

@dataclass(frozen=True)
class SummaryData():
    readable_period: str
    summary_df: pd.DataFrame
    cpu_df: pd.DataFrame
    gpu_df: pd.DataFrame

    def __str__(self):
        
        return f"""Summary for {self.readable_period}
                {'\n  '.join(self.summary_df.to_string().split('\n'))}

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
                """

class SummaryDriver(AnalysisDriverPlugin):
    # Workaround to point to the global definition of SummaryAnalysis instead of the file definition
    SERVED_TYPE=pkg_driver.SummaryAnalysis

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

            cpu_src_id = GrafanaIdentifier(start_ts, end_ts, "cpu", "monthly")
            gpu_src_id = GrafanaIdentifier(start_ts, end_ts, "gpu", "monthly")

            if(not data_repo.contains(cpu_src_id) or not data_repo.contains(gpu_src_id)):
                raise ValueError(f"Can't summarize {start_ts}-{end_ts} the data_repo is missing either the cpu or gpu SourceIdentifier.")
            
            # Shorthand for the get_data method call for readability
            gd = data_repo.get_data

            # Collect analysis identifiers
            cpuhours = AnalysisIdentifier(cpu_src_id, "cpuhours")
            cpuhourstotal = AnalysisIdentifier(cpuhours, "cpuhourstotal")
            cpujobs = AnalysisIdentifier(cpu_src_id, "cpujobs")
            cpujobstotal = AnalysisIdentifier(cpujobs, "cpujobstotal")
            jobstotal = AnalysisIdentifier(cpujobstotal, "jobstotal")

            gpuhours = AnalysisIdentifier(gpu_src_id, "gpuhours")
            gpuhourstotal = AnalysisIdentifier(gpuhours, "gpuhourstotal")
            gpujobs = AnalysisIdentifier(gpu_src_id, "gpujobs")
            gpujobstotal = AnalysisIdentifier(gpujobs, "gpujobstotal")

            # Create the summary data frame
            summary_df = pd.DataFrame({
                "Analysis": ["CPU Only Jobs", "GPU Jobs", "Jobs Total", "CPU Hours", "GPU Hours"],
                "Value": [gd(cpujobstotal), gd(gpujobstotal), gd(jobstotal), gd(cpuhourstotal), gd(gpuhourstotal)]
            })

            # Generate top 5 hours dataframes
            if("top5hours_blacklist" in config_section.keys()):
                top5_blacklist = config_section['top5hours_blacklist']
            else:
                top5_blacklist = []

            cpu_df = gd(cpuhours)
            cpu_df = cpu_df[cpu_df.apply(lambda row: row.iloc[0] not in top5_blacklist, axis=1)].iloc[0:5].reset_index(drop=True)

            gpu_df = gd(gpuhours)
            gpu_df = gpu_df[gpu_df.apply(lambda row: row.iloc[0] not in top5_blacklist, axis=1)].iloc[0:5].reset_index(drop=True)

            readable_period = get_range_printable(cpu_src_id.start_ts, cpu_src_id.end_ts, 3600)
            summary_data = SummaryData(
                readable_period=readable_period,
                summary_df=summary_df,
                cpu_df=cpu_df,
                gpu_df=gpu_df
            )

            data_repo.add(summary_id, summary_data)