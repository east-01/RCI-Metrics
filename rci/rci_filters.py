from src.data.filters import *

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