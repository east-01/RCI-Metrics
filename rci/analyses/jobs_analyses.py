from plugins.rci.analyses.impls.jobs import analyze_cpu_only_jobs_byns, analyze_jobs_byns, analyze_jobs_total, analyze_all_jobs_total
from plugins.rci.rci_filters import filter_source_type, grafana_analysis_key
from src.builtin_plugins.meta_analysis_driver import MetaAnalysis
from src.builtin_plugins.simple_analysis_driver import SimpleAnalysis
from src.builtin_plugins.vis_analysis_driver import VisualAnalysis, VisBarSettings
from src.data.filters import *
from src.plugin_mgmt.plugins import AnalysisPlugin

class JobsAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
            SimpleAnalysis(
                name="cpujobs", 
                prereq_analyses=["gpujobs"],
                filter=filter_source_type("cpu"),
                method=analyze_cpu_only_jobs_byns
            ),
            SimpleAnalysis(
                name="cpujobstotal", 
                prereq_analyses=["cpujobs"],
                filter=filter_analyis_type("cpujobs"),
                method=analyze_jobs_total
            ),
            VisualAnalysis(
                name="viscpujobs",
                prereq_analyses=["cpujobstotal"],
                filter=filter_analyis_type("cpujobs"),
                vis_settings=VisBarSettings(
                    title="Total CPU Jobs by Namespace from %MONTH%",
                    subtext="Total CPU Jobs: %TOTCPUJOBS%",
                    color="skyblue",
                    variables={
                        "TOTCPUJOBS": "cpujobstotal"
                    }
                )
            ), 
            SimpleAnalysis(
                name="gpujobs", 
                prereq_analyses=None,
                filter=filter_source_type("gpu"),
                method=analyze_jobs_byns
            ),
            SimpleAnalysis(
                name="gpujobstotal", 
                prereq_analyses=["gpujobs"],
                filter=filter_analyis_type("gpujobs"),
                method=analyze_jobs_total
            ),
            VisualAnalysis(
                name="visgpujobs",
                prereq_analyses=["gpujobstotal"],
                filter=filter_analyis_type("gpujobs"),
                vis_settings=VisBarSettings(
                    title="Total GPU Hours by Namespace from %MONTH%",
                    subtext="Total GPU Jobs: %TOTGPUJOBS%",
                    color="skyblue",
                    variables={
                        "TOTGPUJOBS": "gpujobstotal"
                    }
                )
            ),
            SimpleAnalysis(
                name="jobstotal", 
                prereq_analyses=["cpujobstotal", "gpujobstotal"],
                filter=filter_analyis_type("cpujobstotal"),
                method=analyze_all_jobs_total
            ),
			MetaAnalysis(
				name="cvgpujobs",
                prereq_analyses=["cpujobstotal", "gpujobstotal"],
                key_method=grafana_analysis_key
			)
        ]