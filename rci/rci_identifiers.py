from dataclasses import dataclass

from data.identifier import TimeStampIdentifier
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

@dataclass(frozen=True)
class GrafanaIntermediateIdentifier(GrafanaIdentifier):
    query_type: str # Talks abt the type of configuration, like status or values

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.query_type))

    def __eq__(self, other) -> bool:
        return isinstance(other, GrafanaIntermediateIdentifier) and super().__eq__(other) and self.query_type == other.query_type

    def __str__(self) -> str:
        return f"intermediate grafana {self.query_cfg}, {self.query_type}, {self.type}, {get_range_printable(self.start_ts, self.end_ts, 3600)}"