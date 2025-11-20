import os
import pandas as pd

from plugins.rci_plugins.analyses.summary_driver import SummaryData
from plugins.rci_plugins.rci_identifiers import SummaryIdentifier
from src.data.data_repository import DataRepository
from src.data.filters import *
from src.plugin_mgmt.plugins import Saver
from src.program_data import ProgramData
from src.utils.timeutils import from_unix_ts_as_monthday, from_unix_ts_as_monthyear

class SummarySaver(Saver):
    def save(self, prog_data: ProgramData, config_section: dict, base_path: str):
        
        data_repo: DataRepository = prog_data.data_repo

        if(not os.path.exists(base_path)):
            os.makedirs(base_path, exist_ok=True)

        summary_filepath = os.path.join(base_path, f"Summary.xlsx")
        identifiers = data_repo.filter_ids(filter_type(SummaryIdentifier))
        with pd.ExcelWriter(summary_filepath, engine="xlsxwriter") as writer:

            for identifier in identifiers:
                summary_data: SummaryData = data_repo.get_data(identifier)

                sheet_name = from_unix_ts_as_monthyear(identifier.start_ts)

                try:
                    write_new_method(identifier, writer, sheet_name, summary_data)
                except PermissionError:
                    print("ERROR: Excel may still be open. Close it if needed.")
                
                print(f"  Saving summary file \"{summary_filepath}\"")

        return [summary_filepath]

def write_new_method(identifier, writer, sheet_name, summary_data: SummaryData):
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet

        # --- Define formats ---
        title_fmt = workbook.add_format({
            'bold': True, 'font_size': 14, 'valign': 'vcenter'
        })
        section_fmt = workbook.add_format({
            'bold': True, 'font_size': 12, 'valign': 'vcenter',
            'bg_color': '#E2EFDA', 'border': 1
        })
        bold_fmt = workbook.add_format({'bold': True})
        num_fmt = workbook.add_format({'num_format': '#,##0.00'})

        space_between = 0
        row = 0

        # ---- Title ----
        start_monthday = from_unix_ts_as_monthday(identifier.start_ts)
        end_monthday = from_unix_ts_as_monthday(identifier.end_ts)
        worksheet.merge_range('A1:C1', f"Compute Hours for {start_monthday} - {end_monthday}", title_fmt)
        row += 1 + space_between

        # ---- Top 5 Jupyterhub Users ----
        worksheet.merge_range(row, 0, row, 2, "Top 5 Jupyterhub Users", section_fmt)
        row += 1 + space_between

        worksheet.merge_range(row, 1, row, 2, "Top GPU Users", bold_fmt)
        summary_data.gpu_jh_users_df.to_excel(
            writer, sheet_name=sheet_name, startrow=row + 1, startcol=1, index=False, header=True
        )
        row += len(summary_data.gpu_jh_users_df) + 2 + space_between

        worksheet.merge_range(row, 1, row, 2, "Top CPU Users", bold_fmt)
        summary_data.cpu_jh_users_df.to_excel(
            writer, sheet_name=sheet_name, startrow=row + 1, startcol=1, index=False, header=True
        )
        row += len(summary_data.cpu_jh_users_df) + 2 + space_between

        # ---- Top 5 Namespaces ----
        worksheet.merge_range(row, 0, row, 2, "Top 5 Namespaces", section_fmt)
        row += 1 + space_between

        worksheet.merge_range(row, 1, row, 2, "Top 5 GPU Hours", bold_fmt)
        summary_data.gpu_df.to_excel(
            writer, sheet_name=sheet_name, startrow=row + 1, startcol=1, index=False, header=True
        )
        row += len(summary_data.gpu_df) + 2 + space_between

        worksheet.merge_range(row, 1, row, 2, "Top 5 CPU Hours", bold_fmt)
        summary_data.cpu_df.to_excel(
            writer, sheet_name=sheet_name, startrow=row + 1, startcol=1, index=False, header=True
        )
        row += len(summary_data.cpu_df) + 2 + space_between

        # ---- Jobs ----
        worksheet.merge_range(row, 0, row, 2, "Jobs", section_fmt)
        row += 1 + space_between
        jobs_df = pd.DataFrame({
            "Category": ["CPU Only Jobs", "GPU Jobs", "Jobs Total"],
            "Count": [summary_data.cpujobstotal, summary_data.gpujobstotal, summary_data.jobstotal]
        })
        jobs_df.to_excel(writer, sheet_name=sheet_name, startrow=row, startcol=1, index=False)
        row += len(jobs_df) + 1 + space_between

        # ---- Compute Hours ----
        worksheet.merge_range(row, 0, row, 2, "Compute Hours", section_fmt)
        row += 1 + space_between
        compute_df = pd.DataFrame({
            "Type": ["CPU Hours", "GPU Hours"],
            "Hours": [summary_data.cpuhourstotal, summary_data.gpuhourstotal]
        })
        compute_df.to_excel(writer, sheet_name=sheet_name, startrow=row, startcol=1, index=False)
        row += len(compute_df) + 1 + space_between

        # ---- TB of Storage Used ----
        worksheet.merge_range(row, 0, row, 2, "TB of Storage Used", section_fmt)
        row += 1 + space_between
        storage_df = pd.DataFrame({
            # Removing CSUSB because we don't get that info
            # "Site": ["TIDE", "CSUSB"],
            # "TB Used": [round(summary_data.usedcapacity, 2), None]
            "Site": ["TIDE"],
            "TB Used": [round(summary_data.usedcapacity, 2)]
        })
        storage_df.to_excel(writer, sheet_name=sheet_name, startrow=row, startcol=1, index=False)
        row += len(storage_df) + 1 + space_between

        # ---- Total number of access granted on JupyterHubs ----
        worksheet.merge_range(row, 0, row, 2, "Total number of access granted on JupyterHubs", section_fmt)
        row += 1 + space_between
        schools = ["San Diego State University",
                   "California State University, San Bernardino",
                   "California State Polytechnic University, Humboldt",
                   "San Francisco State University",
                   "California State University, Sacramento",
                   "California State University San Marcos",
                   "California State University, Northridge",
                   "California State University, Fullerton",
                   "California State University, Chico",
                   "California State University, Stanislaus",
                   "California State University, Fresno",
                   "San José State University",
                   "California State University, Los Angeles",
                   "California State University Channel Islands",
                   "California State Polytechnic University, Pomona",
                   "California State University, Long Beach",
                   "California State University, Dominguez Hills",
                   "California Polytechnic State University, San Luis Obispo",
                   "California State University, Office of the Chancellor",
                   "Sonoma State University",
                   "California State University, Bakersfield",
                   "California State University, East Bay",
                   "California State University, Monterey Bay",
                   "California State University Maritime Academy"]
        access_df = pd.DataFrame({
            "Total": schools,
            f"=SUM(C{row+2}:C{row+1+len(schools)})": [None]*len(schools)
        })
        access_df.to_excel(writer, sheet_name=sheet_name, startrow=row, startcol=1, index=False)

        # ---- Formatting / Column widths ----
        worksheet.set_column("A:A", 10)
        worksheet.set_column("B:B", 45)
        worksheet.set_column("C:C", 12)