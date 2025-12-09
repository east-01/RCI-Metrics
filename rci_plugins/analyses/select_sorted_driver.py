from dataclasses import dataclass
from typing import Callable
import pandas as pd

from plugins.rci_plugins.rci_identifiers import GrafanaIdentifier, SummaryIdentifier
from src.data.data_repository import DataRepository
from src.data.filters import filter_type
from src.data.identifier import AnalysisIdentifier, Identifier
from src.parameter_utils import ConfigurationException
from src.plugin_mgmt.plugins import Analysis, AnalysisDriverPlugin
from src.program_data import ProgramData

import plugins.rci_plugins.analyses.select_sorted_driver as pkg

@dataclass(frozen=True)
class SelectSortedAnalysis(Analysis):
    """ This analysis will take in a DataFrame, a filtering column and a ranking column, then apply
            the filtering and ranking. """
    filter: Callable[[Identifier], bool]
    filter_col_name: str
    filter_col_method: Callable[[pd.Series], bool] # The input is a pd.Series representing the row, and the bool output is whether to keep the row
    rank_col_name: str
    rank_col_ascending: bool
    keep_n: int

class SelectSortedDriver(AnalysisDriverPlugin):
    # pkg. is a workaround to point to the global definition of SelectSortedAnalysis instead of the file definition
    SERVED_TYPE=pkg.SelectSortedAnalysis

    def run_analysis(self, analysis, prog_data: ProgramData, config_section: dict):
        data_repo: DataRepository = prog_data.data_repo

        has_filter = analysis.filter_col_name is not None and analysis.filter_col_method is not None
        # Ensure that, if we don't have a filter, something isn't partially set
        if(not has_filter and (analysis.filter_col_name is not None or analysis.filter_col_method is not None)):
            raise Exception(f"SelectSortedDriver filtering column expects both filter_col_name and filter_col_method to be set, but only one was set.")
        
        has_rank = analysis.rank_col_name is not None

        identifiers = data_repo.filter_ids(analysis.filter)
        for identifier in identifiers:
            df = data_repo.get_data(identifier).copy()

            # Check if data retrieved from repo is a DataFrame
            if(not isinstance(df, pd.DataFrame)):
                raise Exception(f"SelectSortedDriver expected DataFrame data for identifier {identifier}, but got {type(df)}")

            # Filter on the filter column
            if(has_filter):
                if(analysis.filter_col_name not in df.columns):
                    raise Exception(f"SelectSortedDriver filtering column {analysis.filter_col_name} not found in DataFrame columns: {df.columns.tolist()}")

                mask = df.apply(analysis.filter_col_method, axis=1)
                df = df.loc[mask]                
            
            # Rank on the rank column
            if(has_rank):
                if(analysis.rank_col_name not in df.columns):
                    raise Exception(f"SelectSortedDriver ranking column {analysis.rank_col_name} not found in DataFrame columns: {df.columns.tolist()}")

                ascending = False
                if(analysis.rank_col_ascending is not None):
                    ascending = analysis.rank_col_ascending

                df = df.sort_values(by=analysis.rank_col_name, ascending=ascending)

                if(analysis.keep_n is not None):
                    df = df.iloc[0:analysis.keep_n].reset_index(drop=True)

            new_id = AnalysisIdentifier(on=identifier, analysis=analysis.name)
            data_repo.add(new_id, df)
                