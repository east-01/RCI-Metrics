from plugins.rci.rci_filters import *
from src.plugin_mgmt.plugins import Analysis, AnalysisPlugin
from src.plugin_mgmt.builtin.builtin_analyses import SimpleAnalysis, MetaAnalysis
from plugins.rci.analysis_impls.hours import analyze_hours_byns, analyze_hours_total, analyze_available_hours
from plugins.rci.analysis_impls.jobs import analyze_cpu_only_jobs_byns, analyze_jobs_byns, analyze_jobs_total, analyze_all_jobs_total
from plugins.rci.rci_analysis_types import AvailHoursAnalysis

class RCIAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
            SimpleAnalysis(
                name="cpuhours", 
                prereq_analyses=None,
                vis_options=None,
                filter=filter_source_type("cpu"),
                method=analyze_hours_byns
            ),
            SimpleAnalysis(
                name="cpuhourstotal", 
                prereq_analyses=["cpuhours", "cpuhoursavailable"],
                vis_options=None,
                filter=filter_analyis_type("cpuhours"),
                method=analyze_hours_total
            ),
            AvailHoursAnalysis(
                name="cpuhoursavailable", 
                prereq_analyses=None,
                vis_options=None
            ),
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
            )
            
        ]