from src.plugin_mgmt.plugins import AnalysisDriverPlugin
from src.data.data_repository import DataRepository
from src.program_data.config import ConfigurationException
from src.program_data.program_data import ProgramData
from src.data.identifier import TimeStampIdentifier
from src.data.filters import filter_type
from plugins.rci.rci_analysis_types import AvailHoursAnalysis
from plugins.rci.rci_identifiers import AvailableHoursIdentifier

def calculate_total_hours(start_ts, end_ts, type, node_infos):
    """
    Helper method to calculate the total hours available in the specified time frame with the
        specified node information.

    
    """

    # Calculate hour amount
    total_hours_month = (end_ts-start_ts+1)/3600

    if(type=="cpu"):

        cpu_info = node_infos["rci-tide-cpu"]
        cpu_cpus_avail = cpu_info["node_cnt"] * (cpu_info["cpu"]-2)

        tide_gpu_info = node_infos["rci-tide-gpu"]
        gpu_cpus_avail = tide_gpu_info["node_cnt"] * (tide_gpu_info["cpu"]-2)

        cpus_avail = cpu_cpus_avail + gpu_cpus_avail

        return cpus_avail * total_hours_month

    elif(type=="gpu"):

        tide_gpu_info = node_infos["rci-tide-gpu"]
        gpus_avail = tide_gpu_info["node_cnt"] * tide_gpu_info["gpu"]
        
        return gpus_avail * total_hours_month
    
class AvailableHoursDriver(AnalysisDriverPlugin):
    SERVED_TYPE=AvailHoursAnalysis

    def verify_config_section(self, config_section: dict):
        for prim_key in config_section.keys():
            prim_subsect = config_section[prim_key]

            expected_keys_primary = ["rci-tide-cpu", "rci-tide-gpu", "rci-nrp-gpu"]
            if(not set(expected_keys_primary) == set(prim_subsect.keys())):
                raise ConfigurationException(f"The config section \"{prim_key}\" didn't have the right node names- expected: {", ".join(expected_keys_primary)}.")

            for sub_key in expected_keys_primary:
                sub_subsect = prim_subsect[sub_key]

                expected_keys_secondary = ["cpu", "gpu", "node_cnt"]
                if(not set(expected_keys_secondary) == set(sub_subsect.keys())):
                    raise ConfigurationException(f"The sub-config section \"{prim_key}.{sub_key}\" didn't have the right keys- expected: {", ".join(expected_keys_secondary)}.")
                
        return True

    def run_analysis(self, analysis, prog_data: ProgramData, config_section: dict):

        type = analysis.name.replace("hoursavailable", "")
        if(type != "cpu" and type != "gpu"):
            raise Exception(f"Not sure how to handle hours available analysis named \"{analysis.name}\" it should by <type>hoursavailable, so we're looking at {type}")

        data_repo: DataRepository = prog_data.data_repo

        identifiers = data_repo.filter_ids(filter_type(TimeStampIdentifier, True))
        identifiers.sort(key=lambda id: id.start_ts)

        for node_configuration in config_section.keys():
            node_infos = config_section[node_configuration]

            for ts_id in identifiers:
                start_ts = ts_id.start_ts
                end_ts = ts_id.end_ts

                avail = calculate_total_hours(start_ts, end_ts, type, node_infos)

                out_id = AvailableHoursIdentifier(ts_id, analysis.name, type, node_configuration)
                data_repo.add(out_id, avail)