import os
import yaml

from plugins.rci.promql.query_ingest import run
from src.data.data_repository import DataRepository
from src.plugin_mgmt.plugins import IngestPlugin
from src.program_data import ProgramData
from src.parameter_utils import ConfigurationException

class PromQLIngestController(IngestPlugin):
    def verify_config_section(self, config_section) -> bool:
        def check_sec_exists(section):
            if(section not in config_section):
                raise ConfigurationException(f"The section \"{section}\" is not in the config section.")

        check_sec_exists("query-cfgs")

        if(not isinstance(config_section["query-cfgs"], list)):
            raise ConfigurationException("The query_cfgs section is not in the form of a list. PromQLIngestController query-cfgs section expects a list of names for query configs stored in the plugin_dir/ingest_configs/ directory.")

        dir_path = os.path.dirname(os.path.abspath(__file__))
        for cfg_name in config_section["query-cfgs"]:
            file_path = os.path.join(dir_path, "ingest_configs", f"{cfg_name}.yaml")
            if(not os.path.exists(file_path)):
                raise ConfigurationException(f"Failed to find ingest config named \"{cfg_name}\" was expecting to find the file at: {file_path}")

    def ingest(self, prog_data: ProgramData, config_section: dict) -> DataRepository:
        
        data_repo = DataRepository()

        dir_path = os.path.dirname(os.path.abspath(__file__))
        for cfg_name in config_section["query-cfgs"]:
            file_path = os.path.join(dir_path, "ingest_configs", f"{cfg_name}.yaml")
            if(not os.path.exists(file_path)):
                raise Exception(f"Failed to find ingest config named \"{cfg_name}\" was expecting to find the file at: {file_path}")
            
            with open(file_path, "r") as file:
                data = yaml.safe_load(file)
            
            data["cfg_name"] = cfg_name

            period_list = prog_data.timeline.periods
            if("main-periods" in config_section and config_section["main-periods"] is True):
                period_list = prog_data.timeline.main_periods

            returned_data_repo = run(data, period_list)

            # Save a data_repo.join() step if the existing repo is already empty
            if(data_repo.count() > 0):
                data_repo.join(returned_data_repo)
            else:
                data_repo = returned_data_repo

        return data_repo

