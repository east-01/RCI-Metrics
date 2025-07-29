from plugins.builtin.builtin_analyses import SimpleAnalysis, MetaAnalysis

from plugins.rci.rci_filters import filter_source_type
from src.plugin_mgmt.plugins import AnalysisPlugin

class RCIAnalyses(AnalysisPlugin):
    def get_analyses(self):
        return [
            SimpleAnalysis(
                name="cpuhours", 
                prereq_analyses=None,
                # required_ingests="grafana.*", 
                filter=filter_source_type("cpu")
            )
            # Analysis(
            #     name="cpuhours_csu", 
            #     prereq_analyses=None,
            #     required_ingests="grafana.csu", 
            #     filter=filter_source_type("cpu")            
            # )
        ]