from plugins.rci_plugins.analyses.select_sorted_driver import SelectSortedAnalysis
from plugins.rci_plugins.analyses.summary_driver import SummaryAnalysis
from plugins.rci_plugins.analyses.impls.namespaces import analyze_uniquens, analyze_all_uniquens
from plugins.rci_plugins.analyses.impls.usedcapacity import analyze_usedcapacity
from plugins.rci_plugins.analyses.tidesplit_driver import TideSplitAnalysis, aggregate_tide_split
from plugins.rci_plugins.rci_identifiers import GrafanaIdentifier
from src.builtin_plugins.agg_analysis_driver import AggregateAnalysis
from src.builtin_plugins.meta_analysis_driver import MetaAnalysis
from src.builtin_plugins.simple_analysis_driver import SimpleAnalysis
from src.data.filters import *
from src.plugin_mgmt.plugins import AnalysisPlugin

import pandas as pd
def filter_sdsu_emails(row: pd.Series):
    return row["Namespace"].endswith("@sdsu.edu")
def filter_sdsu_namespaces(row: pd.Series):
    return row["Namespace"].startswith("sdsu-")

class MiscAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
            SimpleAnalysis(
                name="uniquens",
                prereq_analyses=None,
                filter=filter_type(GrafanaIdentifier, strict=True),
                method=analyze_uniquens
            ),
            AggregateAnalysis(
                name="alluniquens",
                prereq_analyses=["uniquens"],
                key_method=None,
                filter=filter_analyis_type("uniquens"),
                method=analyze_all_uniquens
            ),
            
            SummaryAnalysis(
                name="summary",
                prereq_analyses=["cpuhours", "gpuhours", "jobstotal", "cpuhourstotal", "gpuhourstotal", "cpujhpodhours", "gpujhpodhours", "usedcapacity"]
            ),

            MetaAnalysis(
				name="utilization",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable", "gpuhourstotal", "gpuhoursavailable"],
                key_method=None
			),
            TideSplitAnalysis(
                name="tidesplit",
                prereq_analyses=["cpuhourstotal", "gpuhourstotal", "cpuhoursavailable", "gpuhoursavailable"]
            ),
            AggregateAnalysis(
                name="tidesplitmeta",
                prereq_analyses=["tidesplit"],
                key_method=None,
                filter=filter_analyis_type("tidesplit"),
                method=aggregate_tide_split
            ),

            SimpleAnalysis(
                name="usedcapacity",
                prereq_analyses=None,
                filter=lambda id: filter_type(GrafanaIdentifier)(id) and id.type=="storage",
                method=analyze_usedcapacity
            ),

            SelectSortedAnalysis(
                name="topsdsuusers_cpu",
                prereq_analyses=["cpujhpodhours"],
                filter=lambda id: filter_analyis_type("cpujhpodhours")(id) and id.find_base().query_cfg == "jupyterhub",
                filter_col_name="Namespace",
                filter_col_method=filter_sdsu_emails,
                rank_col_name="Hours",
                rank_col_ascending=False,
                keep_n=10
            ),
            SelectSortedAnalysis(
                name="topsdsuusers_gpu",
                prereq_analyses=["gpujhpodhours"],
                filter=lambda id: filter_analyis_type("gpujhpodhours")(id) and id.find_base().query_cfg == "jupyterhub",
                filter_col_name="Namespace",
                filter_col_method=filter_sdsu_emails,
                rank_col_name="Hours",
                rank_col_ascending=False,
                keep_n=10
            ),
            SelectSortedAnalysis(
                name="topsdsunamespaces_cpu",
                prereq_analyses=["cpuhours"],
                filter=lambda id: filter_analyis_type("cpuhours")(id) and id.find_base().query_cfg == "monthly",
                filter_col_name="Namespace",
                filter_col_method=filter_sdsu_namespaces,
                rank_col_name="Hours",
                rank_col_ascending=False,
                keep_n=10
            ),
            SelectSortedAnalysis(
                name="topsdsunamespaces_gpu",
                prereq_analyses=["gpuhours"],
                filter=lambda id: filter_analyis_type("gpuhours")(id) and id.find_base().query_cfg == "monthly",
                filter_col_name="Namespace",
                filter_col_method=filter_sdsu_namespaces,
                rank_col_name="Hours",
                rank_col_ascending=False,
                keep_n=10
            )
        ]