from src.data.filters import *
from src.plugin_mgmt.plugins import AnalysisPlugin
from src.plugins_builtin.meta_analysis_driver import MetaAnalysis
from src.plugins_builtin.simple_analysis_driver import SimpleAnalysis
from src.plugins_builtin.verification_analysis_driver import VerificationAnalysis
from src.plugins_builtin.vis_analysis_driver import VisualAnalysis, VisBarSettings, VisTimeSettings
from plugins.rci.analyses.impls.hours import analyze_hours_byns, analyze_hours_total, verify_hours
from plugins.rci.analyses.impls.jobs import analyze_cpu_only_jobs_byns, analyze_jobs_byns, analyze_jobs_total, analyze_all_jobs_total
from plugins.rci.analyses.available_hours_driver import AvailHoursAnalysis
from plugins.rci.analyses.summary_driver import SummaryAnalysis
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
                filter=filter_source_type("cpu"),
                method=analyze_hours_byns
            ),
            SimpleAnalysis(
                name="cpuhourstotal", 
                prereq_analyses=["cpuhours"],
                filter=filter_analyis_type("cpuhours"),
                method=analyze_hours_total
            ),
            AvailHoursAnalysis(
                name="cpuhoursavailable", 
                prereq_analyses=None
            ),
            VerificationAnalysis(
                name="verifycpuhours",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable"],
                targ_analysis="cpuhourstotal",
                method=verify_hours
            ),
            VisualAnalysis(
                name="viscpuhours",
                prereq_analyses=["cpuhourstotal"],
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
            MetaAnalysis(
                name="cpuhoursmeta",
                prereq_analyses=["cpuhourstotal"],
                key_method=grafana_analysis_key
            ),
            VisualAnalysis(
                name="viscpuhoursmeta",
                prereq_analyses=["cpuhoursmeta"],
                filter=filter_analyis_type("cpuhoursmeta"),
                vis_settings=VisTimeSettings(
                    title="CPU hours by month",
                    variables=None,
                    color={
                        "cpuhourstotal": "red"
                    }
                )
            ),
            SimpleAnalysis(
                name="gpuhours", 
                prereq_analyses=None,
                filter=filter_source_type("gpu"),
                method=analyze_hours_byns
            ),
            SimpleAnalysis(
                name="gpuhourstotal", 
                prereq_analyses=["gpuhours", "gpuhoursavailable"],
                filter=filter_analyis_type("gpuhours"),
                method=analyze_hours_total
            ),
            AvailHoursAnalysis(
                name="gpuhoursavailable", 
                prereq_analyses=None
            ),
            VerificationAnalysis(
                name="verifygpuhours",
                prereq_analyses=["gpuhourstotal", "gpuhoursavailable"],
                targ_analysis="gpuhourstotal",
                method=verify_hours
            ),
            VisualAnalysis(
                name="visgpuhours",
                prereq_analyses=["gpuhourstotal"],
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
            MetaAnalysis(
                name="gpuhoursmeta",
                prereq_analyses=["gpuhourstotal"],
                key_method=grafana_analysis_key
            ),
            VisualAnalysis(
                name="visgpuhoursmeta",
                prereq_analyses=["gpuhoursmeta"],
                filter=filter_analyis_type("gpuhoursmeta"),
                vis_settings=VisTimeSettings(
                    title="GPU hours by month",
                    variables=None,
                    color={
                        "gpuhourstotal": "blue"
                    }
                )
            ),
            MetaAnalysis(
				name="cvgpuhours",
                prereq_analyses=["cpuhourstotal", "gpuhourstotal"],
                key_method=grafana_analysis_key
			),
            VisualAnalysis(
                name="viscvgpuhours",
                prereq_analyses=["cvgpuhours"],
                filter=filter_analyis_type("cvgpuhours"),
                vis_settings=VisTimeSettings(
                    title="CPU and GPU hours by month",
                    variables=None,
                    color={
                        "cpuhourstotal": "red",
                        "gpuhourstotal": "blue"
                    }
                )
            ),
#endregion
#region Jobs
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
			),
#endregion
#region Misc
            MetaAnalysis(
				name="utilization",
                prereq_analyses=["cpuhourstotal", "cpuhoursavailable", "gpuhourstotal", "gpuhoursavailable"],
                key_method=None
			),
            SummaryAnalysis(
                name="summary",
                prereq_analyses=["cpuhours", "gpuhours", "jobstotal", "cpuhourstotal", "gpuhourstotal"]
            )
#endregion
        ]