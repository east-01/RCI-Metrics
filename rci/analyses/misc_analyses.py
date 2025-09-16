from plugins.rci.analyses.summary_driver import SummaryAnalysis
from src.builtin_plugins.meta_analysis_driver import MetaAnalysis
from src.data.filters import *
from src.plugin_mgmt.plugins import AnalysisPlugin

class MiscAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
            MetaAnalysis(
				name="utilization",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable", "gpuhourstotal", "gpuhoursavailable"],
                key_method=None
			),
            SummaryAnalysis(
                name="summary",
                prereq_analyses=["cpuhours", "gpuhours", "jobstotal", "cpuhourstotal", "gpuhourstotal"]
            )
        ]