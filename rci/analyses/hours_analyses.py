from plugins.rci.analyses.available_hours_driver import AvailHoursAnalysis
from plugins.rci.analyses.impls.hours import analyze_hours_byns, analyze_hours_total, verify_hours
from plugins.rci.rci_filters import filter_source_type, grafana_analysis_key
from src.builtin_plugins.meta_analysis_driver import MetaAnalysis
from src.builtin_plugins.simple_analysis_driver import SimpleAnalysis
from src.builtin_plugins.verification_analysis_driver import VerificationAnalysis
from src.builtin_plugins.vis_analysis_driver import VisualAnalysis, VisBarSettings, VisTimeSettings
from src.data.filters import *
from src.plugin_mgmt.plugins import AnalysisPlugin

class HoursAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
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
            )
        ]