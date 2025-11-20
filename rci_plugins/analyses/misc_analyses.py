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
            )
        ]