import os
import pandas as pd

from src.program_data import ProgramData
from src.data.data_repository import DataRepository
from src.data.filters import *
from src.plugin_mgmt.plugins import Saver
from plugins.rci.rci_identifiers import GrafanaIdentifier, SummaryIdentifier
from plugins.rci.analyses.summary_driver import SummaryData

class SummarySaver(Saver):
    def save(self, prog_data: ProgramData, config_section: dict, base_path: str):
        
        data_repo: DataRepository = prog_data.data_repo

        all_saved_files = []

        identifiers = data_repo.filter_ids(filter_type(SummaryIdentifier))
        for identifier in identifiers:
            summary_data: SummaryData = data_repo.get_data(identifier)

            try:
                
                summary_filepath = os.path.join(base_path, f"{identifier.fs_str()}.xlsx")
                with pd.ExcelWriter(summary_filepath, engine="xlsxwriter") as writer:
                    summary_data.summary_df.to_excel(writer, sheet_name="Summary", index=False)
                    summary_data.cpu_df.to_excel(writer, sheet_name="Top5 CPU NS", index=False)
                    summary_data.gpu_df.to_excel(writer, sheet_name="Top5 GPU NS", index=False)

                    worksheet = writer.sheets["Summary"]
                    worksheet.set_column(0, 0, 20)

                all_saved_files.append(summary_filepath)
                
                print(f"  Saving summary file \"{summary_filepath}\"")

            except PermissionError:
                print("ERROR: Recieved PermissionError when trying to save summary excel spreadsheet. This can happen if the sheet is open in another window (close excel).")

        return all_saved_files

                
