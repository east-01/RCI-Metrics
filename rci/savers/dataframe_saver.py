import os

from plugins.rci.rci_identifiers import GrafanaIdentifier
from src.data.data_repository import DataRepository
from src.data.filters import *
from src.plugin_mgmt.plugins import Saver
from src.program_data import ProgramData

class DataFrameSaver(Saver):
    """ Save DataFrames as .csv files in the file system. """

    def save(self, prog_data: ProgramData, config_section: dict, base_path: str):

        data_repo: DataRepository = prog_data.data_repo

        out_path = base_path
        if(not os.path.exists(out_path)):
            os.makedirs(out_path, exist_ok=True)
        
        for identifier in data_repo.filter_ids(filter_type(GrafanaIdentifier)):
            df = data_repo.get_data(identifier)

            # Convert the readable_period into a string thats saveable by the file system
            df_path = os.path.join(out_path, f"{identifier.fs_str()}.csv")
            print(f"  Saving DataFrame file \"{df_path}\"")

            df.to_csv(df_path, index=False)