# import os
# import pandas as pd

# from src.program_data.program_data import ProgramData
# from src.data.data_repository import DataRepository
# from src.data.filters import *
# from src.data.saving.saver import Saver
# from src.data.identifiers.identifier import *

# class SummarySaver(Saver):
#     def __init__(self, prog_data: ProgramData):
#         self.prog_data = prog_data

#     def save(self):
        
#         data_repo: DataRepository = self.prog_data.data_repo

#         outdir = self.prog_data.args.outdir

#         for identifier in data_repo.filter_ids(filter_type(SummaryIdentifier)):
#             identifier: SourceIdentifier = identifier

#             cpu_src_id = SourceIdentifier(identifier.start_ts, identifier.end_ts, "cpu")
#             metadata = data_repo.get_metadata(cpu_src_id)

#             summary_df, cpu_df, gpu_df = data_repo.get_data(identifier)

#             try:
                
#                 summary_filepath = os.path.join(outdir, f"{metadata['readable_period']} summary.xlsx")
#                 with pd.ExcelWriter(summary_filepath, engine="xlsxwriter") as writer:
#                     summary_df.to_excel(writer, sheet_name="Summary", index=False)
#                     cpu_df.to_excel(writer, sheet_name="Top5 CPU NS", index=False)
#                     gpu_df.to_excel(writer, sheet_name="Top5 GPU NS", index=False)

#                     worksheet = writer.sheets["Summary"]
#                     worksheet.set_column(0, 0, 20)

#             except PermissionError:
#                 print("ERROR: Recieved PermissionError when trying to save summary excel spreadsheet. This can happen if the sheet is open in another window (close excel).")

                
