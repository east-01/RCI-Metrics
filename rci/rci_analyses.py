from src.data.filters import *
from src.plugin_mgmt.plugins import Analysis, AnalysisPlugin
from src.plugin_mgmt.builtin.builtin_analyses import SimpleAnalysis, MetaAnalysis, VerificationAnalysis
from src.plugin_mgmt.builtin.visualizations import VisIdentifier, VisualAnalysis, VisBarSettings, VisTimeSettings
from plugins.rci.analysis_impls.hours import analyze_hours_byns, analyze_hours_total, analyze_available_hours, verify_hours
from plugins.rci.analysis_impls.jobs import analyze_cpu_only_jobs_byns, analyze_jobs_byns, analyze_jobs_total, analyze_all_jobs_total
from plugins.rci.available_hours_driver import AvailHoursAnalysis
from plugins.rci.summary_driver import SummaryAnalysis
from plugins.rci.rci_identifiers import GrafanaIdentifier

def filter_source_type(resource_type: str):
    """
    Get a list of SourceIdentifiers that have the same type as resource_type.

    Args:
        resource_type (str): The target resource type for SourceIdentifiers.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    """

    analysis_type_lambda = filter_type(GrafanaIdentifier)
    return lambda identifier: analysis_type_lambda(identifier) and identifier.type == resource_type

grafana_analysis_key = lambda identifier: identifier.find_base().query_cfg

class RCIAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
#region Hours
            SimpleAnalysis(
                name="cpuhours", 
                prereq_analyses=None,
                vis_options=None,
                filter=filter_source_type("cpu"),
                method=analyze_hours_byns
            ),
            SimpleAnalysis(
                name="cpuhourstotal", 
                prereq_analyses=["cpuhours"],
                vis_options=None,
                filter=filter_analyis_type("cpuhours"),
                method=analyze_hours_total
            ),
            AvailHoursAnalysis(
                name="cpuhoursavailable", 
                prereq_analyses=None,
                vis_options=None
            ),
            VerificationAnalysis(
                name="verifycpuhours",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable"],
                vis_options=None,
                targ_analysis="cpuhourstotal",
                method=verify_hours
            ),
            SimpleAnalysis(
                name="gpuhours", 
                prereq_analyses=None,
                vis_options=None,
                filter=filter_source_type("gpu"),
                method=analyze_hours_byns
            ),
            SimpleAnalysis(
                name="gpuhourstotal", 
                prereq_analyses=["gpuhours", "gpuhoursavailable"],
                vis_options=None,
                filter=filter_analyis_type("gpuhours"),
                method=analyze_hours_total
            ),
            AvailHoursAnalysis(
                name="gpuhoursavailable", 
                prereq_analyses=None,
                vis_options=None
            ),
            VerificationAnalysis(
                name="verifygpuhours",
                prereq_analyses=["gpuhourstotal", "gpuhoursavailable"],
                vis_options=None,
                targ_analysis="gpuhourstotal",
                method=verify_hours
            ),
#endregion
#region Jobs
            SimpleAnalysis(
                name="cpujobs", 
                prereq_analyses=["gpujobs"],
                vis_options=None,
                filter=filter_source_type("cpu"),
                method=analyze_cpu_only_jobs_byns
            ),
            SimpleAnalysis(
                name="cpujobstotal", 
                prereq_analyses=["cpujobs"],
                vis_options=None,
                filter=filter_analyis_type("cpujobs"),
                method=analyze_jobs_total
            ),
            SimpleAnalysis(
                name="gpujobs", 
                prereq_analyses=None,
                vis_options=None,
                filter=filter_source_type("gpu"),
                method=analyze_jobs_byns
            ),
            SimpleAnalysis(
                name="gpujobstotal", 
                prereq_analyses=["gpujobs"],
                vis_options=None,
                filter=filter_analyis_type("gpujobs"),
                method=analyze_jobs_total
            ),
            SimpleAnalysis(
                name="jobstotal", 
                prereq_analyses=["cpujobstotal", "gpujobstotal"],
                vis_options=None,
                filter=filter_analyis_type("cpujobstotal"),
                method=analyze_all_jobs_total
            ),
#endregion
#region Meta-analyses
			MetaAnalysis(
				name="cvgpuhours",
                prereq_analyses=["cpuhourstotal", "gpuhourstotal"],
                vis_options=None,
                key_method=grafana_analysis_key
			),
			MetaAnalysis(
				name="cvgpujobs",
                prereq_analyses=["cpujobstotal", "gpujobstotal"],
                vis_options=None,
                key_method=grafana_analysis_key
			),
			MetaAnalysis(
				name="utilization",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable", "gpuhourstotal", "gpuhoursavailable"],
                vis_options=None,
                key_method=None
			),
#endregion
#region Misc
            SummaryAnalysis(
                name="summary",
                prereq_analyses=["cpuhours", "gpuhours", "jobstotal", "cpuhourstotal", "gpuhourstotal"],
                vis_options=None
            ),
#endregion
#region Visualizations
            VisualAnalysis(
                name="viscpuhours",
                prereq_analyses=["cpuhourstotal"],
                vis_options=None,
                filter=filter_analyis_type("cpuhours"),
                vis_settings=VisBarSettings(
                    title="Total CPU Hours by Namespace from %MONTH%",
                    subtext="Total CPU Hours: %TOTCPUHRS%",
                    color="skyblue",
                    variables={
                        "TOTCPUHRS": "cpuhourstotal"
                    }
                )
            ), 
            VisualAnalysis(
                name="viscpujobs",
                prereq_analyses=["cpujobstotal"],
                vis_options=None,
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
            VisualAnalysis(
                name="visgpuhours",
                prereq_analyses=["gpuhourstotal"],
                vis_options=None,
                filter=filter_analyis_type("gpuhours"),
                vis_settings=VisBarSettings(
                    title="Total GPU Hours by Namespace from %MONTH%",
                    subtext="Total GPU Hours: %TOTGPUHRS%",
                    color="orange",
                    variables={
                        "TOTGPUHRS": "gpuhourstotal"
                    }
                )
            ), 
            VisualAnalysis(
                name="visgpujobs",
                prereq_analyses=["gpujobstotal"],
                vis_options=None,
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
            VisualAnalysis(
                name="viscvgpuhours",
                prereq_analyses=["cvgpuhours"],
                vis_options=None,
                filter=filter_analyis_type("cvgpuhours"),
                vis_settings=VisTimeSettings(
                    title="CPU and GPU hours by month",
                    variables=None,
                    color={
                        "cpuhourstotal": "red",
                        "gpuhourstotal": "blue"
                    }
                )
            )
#endregion
        ]