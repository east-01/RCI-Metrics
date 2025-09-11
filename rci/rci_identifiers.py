from dataclasses import dataclass

from src.data.identifier import TimeStampIdentifier, AnalysisIdentifier
from src.utils.timeutils import get_range_printable

@dataclass(frozen=True)
class GrafanaIdentifier(TimeStampIdentifier):
    type: str
    query_cfg: str # Talks abt the configuration of the ingest controller, like monthly or csu

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.type, self.query_cfg))

    def __eq__(self, other) -> bool:
        return isinstance(other, GrafanaIdentifier) and super().__eq__(other) and self.type == other.type and self.query_cfg == other.query_cfg

    def __str__(self) -> str:
        return f"grafana {self.query_cfg}, {self.type}, {get_range_printable(self.start_ts, self.end_ts, 3600)}"
    
    def fs_str(self):
        readable_period = get_range_printable(self.start_ts, self.end_ts, 3600)
        readable_period_cleaned = readable_period.replace("/", "_").replace(" ", "T").replace(":", "")
        return f"{self.type}-{readable_period_cleaned}"

@dataclass(frozen=True)
class GrafanaIntermediateIdentifier(GrafanaIdentifier):
    query_type: str # Talks abt the type of configuration, like status or values

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.query_type))

    def __eq__(self, other) -> bool:
        return isinstance(other, GrafanaIntermediateIdentifier) and super().__eq__(other) and self.query_type == other.query_type

    def __str__(self) -> str:
        return f"intermediate grafana {self.query_cfg}, {self.query_type}, {self.type}, {get_range_printable(self.start_ts, self.end_ts, 3600)}"
    
@dataclass(frozen=True)
class AvailableHoursIdentifier(AnalysisIdentifier):
    type: str
    config: str

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.type, self.config))

    def __eq__(self, other) -> bool:
        return isinstance(other, AvailableHoursIdentifier) and super().__eq__(other) and self.type == other.type and self.config == other.config

    def __str__(self) -> str:
        return f"{self.analysis}({self.on}, {self.type}, {self.config})"
    
@dataclass(frozen=True)
class SummaryIdentifier(TimeStampIdentifier):
    """
    An identifier for a summary of a period, the start_ts and end_ts will match the corresponding
      SourceIdentifiers' start_ts and end_ts.
    """
    def __hash__(self) -> int:
        return hash((self.start_ts, self.end_ts))
    
    def __eq__(self, other) -> bool:
        return isinstance(other, SummaryIdentifier) and super().__eq__(other) and self.start_ts == other.start_ts and self.start_ts == other.start_ts

    def __str__(self) -> str:
        return f"summary of {self.start_ts}-{self.end_ts}"

    def fs_str(self) -> str:
        return f"{get_range_printable(self.start_ts, self.end_ts, 3600)} summary"
