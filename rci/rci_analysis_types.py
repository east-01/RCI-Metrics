from dataclasses import dataclass

from src.plugin_mgmt.plugins import Analysis

@dataclass(frozen=True)
class AvailHoursAnalysis(Analysis):
    pass
