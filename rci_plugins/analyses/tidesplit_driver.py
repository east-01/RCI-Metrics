from dataclasses import dataclass
import pandas as pd

from plugins.rci_plugins.rci_filters import grafana_analysis_key
from plugins.rci_plugins.rci_identifiers import TideSplitIdentifier
from src.data.data_repository import DataRepository
from src.data.filters import filter_type
from src.data.identifier import TimeStampIdentifier
from src.plugin_mgmt.plugins import Analysis, AnalysisDriverPlugin
from src.program_data import ProgramData
from src.utils.datautils import resolve_analysis
from src.utils.timeutils import get_range_printable

import plugins.rci_plugins.analyses.tidesplit_driver as pkg

@dataclass(frozen=True)
class TideSplitAnalysis(Analysis):
    """ A wrapper for the standard analysis so we can capture it with the SummaryDriver. """
    pass

class TideSplitDriver(AnalysisDriverPlugin):
    # Workaround to point to the global definition of SummaryAnalysis instead of the file definition
    SERVED_TYPE=pkg.TideSplitAnalysis

    def run_analysis(self, analysis, prog_data: ProgramData, config_section: dict):
        data_repo: DataRepository = prog_data.data_repo
    
        # CSU v NRP cpu, tainted v untainted cpu, CSU v NRP gpu, tainted v untainted gpu 

        ts_identifiers = data_repo.filter_ids(filter_type(TimeStampIdentifier, strict=True))
        ts_identifiers.sort(key=lambda id: id.start_ts)

        for ts_identifier in ts_identifiers:
            for type in ["cpu", "gpu"]:
                data = _get_data(data_repo, ts_identifier, type)
                identifier = TideSplitIdentifier(on=ts_identifier, analysis=analysis.name, type=type)

                data_repo.add(identifier, data)

def _get_data(data_repo: DataRepository, ts_identifier, type):
    available_hours_type_key = lambda id: (id.type, id.config)

    hrs_id_csu = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hourstotal", key_method=grafana_analysis_key, unique_key="csu")
    hrs_id_noncsu = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hourstotal", key_method=grafana_analysis_key, unique_key="non-csu")
    hrs_id_tainted = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hourstotal", key_method=grafana_analysis_key, unique_key="tainted")
    hrs_id_untainted = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hourstotal", key_method=grafana_analysis_key, unique_key="untainted")
    
    avail_hrs_id_default = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hoursavailable", key_method=available_hours_type_key, unique_key=(type, "default"))
    avail_hrs_id_tainted = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hoursavailable", key_method=available_hours_type_key, unique_key=(type, "tainted"))
    avail_hrs_id_untainted = resolve_analysis(data_repo, ts_identifier.start_ts, ts_identifier.end_ts, f"{type}hoursavailable", key_method=available_hours_type_key, unique_key=(type, "untainted"))

    hrs_csu = float(data_repo.get_data(hrs_id_csu)) if data_repo.contains(hrs_id_csu) else 0
    hrs_noncsu = float(data_repo.get_data(hrs_id_noncsu)) if data_repo.contains(hrs_id_noncsu) else 0
    hrs_tainted = float(data_repo.get_data(hrs_id_tainted)) if data_repo.contains(hrs_id_tainted) else 0
    hrs_untainted = float(data_repo.get_data(hrs_id_untainted)) if data_repo.contains(hrs_id_untainted) else 0

    avail_hrs_default = float(data_repo.get_data(avail_hrs_id_default)) if data_repo.contains(avail_hrs_id_default) else 0
    avail_hrs_tainted = float(data_repo.get_data(avail_hrs_id_tainted)) if data_repo.contains(avail_hrs_id_tainted) else 0
    avail_hrs_untainted = float(data_repo.get_data(avail_hrs_id_untainted)) if data_repo.contains(avail_hrs_id_untainted) else 0

    idle_csunoncsu = avail_hrs_default - hrs_csu - hrs_noncsu
    idle_tainted = avail_hrs_tainted - hrs_tainted
    idle_untainted = avail_hrs_untainted - hrs_untainted

    return hrs_csu, hrs_noncsu, idle_csunoncsu, avail_hrs_default, hrs_tainted, idle_tainted, avail_hrs_tainted, hrs_untainted, idle_untainted, avail_hrs_untainted

def aggregate_tide_split(identifiers, data_repo: DataRepository):

    columns = ["Period", 
               "CSU CPU Hours", "Non-CSU CPU Hours", "CPU Hours Idle", "CPU Hours Available",
               "Tainted CPU Hours", "Tainted CPU Hours Idle", "Tainted CPU Hours Available",
               "Untainted CPU Hours", "Untainted CPU Hours Idle", "Untainted CPU Hours Available",
               "CSU GPU Hours", "Non-CSU GPU Hours", "GPU Hours Idle", "GPU Hours Available",
               "Tainted GPU Hours", "Tainted GPU Hours Idle", "Tainted GPU Hours Available",
               "Untainted GPU Hours", "Untainted GPU Hours Idle", "Untainted GPU Hours Available"]

    identifiers.sort(key=lambda id: id.find_base().start_ts)
    out_df = pd.DataFrame(columns=columns)

    current_ts = None
    cpu_id = None
    gpu_id = None

    def add_to_df():
        # Create a tuple of zeroes that corresponds with each variable for default purposes
        zero_tuple = (0, 0, 0, 0,
                      0, 0, 0,
                      0, 0, 0)
        
        if(data_repo.contains(cpu_id)):            
            (cpu_hrs_csu, cpu_hrs_noncsu, cpu_idle_csunoncsu, cpu_avail_hrs_default, 
             cpu_hrs_tainted, cpu_idle_tainted, cpu_avail_hrs_tainted, 
             cpu_hrs_untainted, cpu_idle_untainted, cpu_avail_hrs_untainted) = data_repo.get_data(cpu_id)
        else:
            (cpu_hrs_csu, cpu_hrs_noncsu, cpu_idle_csunoncsu, cpu_avail_hrs_default, 
             cpu_hrs_tainted, cpu_idle_tainted, cpu_avail_hrs_tainted, 
             cpu_hrs_untainted, cpu_idle_untainted, cpu_avail_hrs_untainted) = zero_tuple
            print(f"WARNING: Aggregating tide split results failed to find cpu tide split analysis for {current_ts}")

        if(data_repo.contains(gpu_id)):            
            (gpu_hrs_csu, gpu_hrs_noncsu, gpu_idle_csunoncsu, gpu_avail_hrs_default, 
             gpu_hrs_tainted, gpu_idle_tainted, gpu_avail_hrs_tainted, 
             gpu_hrs_untainted, gpu_idle_untainted, gpu_avail_hrs_untainted) = data_repo.get_data(gpu_id)
        else:
            (gpu_hrs_csu, gpu_hrs_noncsu, gpu_idle_csunoncsu, gpu_avail_hrs_default, 
             gpu_hrs_tainted, gpu_idle_tainted, gpu_avail_hrs_tainted, 
             gpu_hrs_untainted, gpu_idle_untainted, gpu_avail_hrs_untainted) = zero_tuple
            print(f"WARNING: Aggregating tide split results failed to find gpu tide split analysis for {current_ts}")

        row = [
            get_range_printable(current_ts.start_ts, current_ts.end_ts),
            cpu_hrs_csu, cpu_hrs_noncsu, cpu_idle_csunoncsu, cpu_avail_hrs_default, 
            cpu_hrs_tainted, cpu_idle_tainted, cpu_avail_hrs_tainted, 
            cpu_hrs_untainted, cpu_idle_untainted, cpu_avail_hrs_untainted,
            gpu_hrs_csu, gpu_hrs_noncsu, gpu_idle_csunoncsu, gpu_avail_hrs_default, 
            gpu_hrs_tainted, gpu_idle_tainted, gpu_avail_hrs_tainted, 
            gpu_hrs_untainted, gpu_idle_untainted, gpu_avail_hrs_untainted
        ]

        out_df.loc[len(out_df)] = row

    def clear_ids():
        global cpu_id, gpu_id
        
        cpu_id = None
        gpu_id = None

    for identifier in identifiers:
        
        looking_at_ts = TimeStampIdentifier(identifier.find_base().start_ts, identifier.find_base().end_ts)

        if(looking_at_ts != current_ts):
            if(current_ts is not None):
                # Add the current timestamps to df
                add_to_df()
                clear_ids()

            current_ts = looking_at_ts
        
        if(identifier.type == "cpu"):
            cpu_id = identifier
        elif(identifier.type == "gpu"):
            gpu_id = identifier
        else:
            raise Exception(f"Don't know how to handle tidesplit identifier type \"{identifier.type}\"")
        
    add_to_df()

    return out_df