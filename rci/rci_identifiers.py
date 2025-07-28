from dataclasses import dataclass

from data.identifier import TimeStampIdentifier

@dataclass(frozen=True)
class GrafanaIdentifier(TimeStampIdentifier):
    type: str
    ingest_src: str # Talks abt either grafana, ingest_filesystem
    ingest_cfg: str # Talks abt the configuration of the ingest controller

@dataclass(frozen=True)
class SourceIdentifier(TimeStampIdentifier):
    """
    Identifier for a source Grafana DataFrame.
    """
    type: str # cpu/gpu

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.type))

    def __eq__(self, other) -> bool:
        return isinstance(other, SourceIdentifier) and super().__eq__(other) and self.type == other.type

    def __str__(self) -> str:
        return f"sourcedata {self.type}:{self.start_ts}-{self.end_ts}"
    
@dataclass(frozen=True)
class SourceQueryIdentifier(SourceIdentifier):
    """
    Identifier for a source Grafana DataFrame. With additional information about the query that
      generated it. Used in PromQL ingest process.
    """
    query_name: str # status/truth (-> values)

    def __hash__(self) -> int:
        return hash((super().__hash__(), self.query_name))

    def __eq__(self, other) -> bool:
        return isinstance(other, SourceQueryIdentifier) and super().__eq__(other) and self.query_name == other.query_name

    def __str__(self) -> str:
        return f"sourcequerydata {self.type} from query {self.query_name}:{self.start_ts}-{self.end_ts}"