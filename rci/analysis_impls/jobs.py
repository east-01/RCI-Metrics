# This code is repackaged from Tide2.ipynb in https://github.com/SDSU-Research-CI/rci-helpful-scripts
import pandas as pd

from src.data.data_repository import DataRepository
from src.data.identifier import AnalysisIdentifier
from plugins.promql.grafana_df_cleaning import clear_duplicate_uids, clear_blacklisted_uids, has_time_column, clear_time_column
from plugins.rci.rci_identifiers import GrafanaIdentifier

def analyze_jobs_byns(identifier, data_repo: DataRepository):
    """
    Unpack the Grafana DataFrame from the DataRepository and perform _analyze_jobs_byns_ondf on it.
    
    Args:
        identifier (GrafanaIdentifier): The identifier for the Grafana DataFrame.
    Returns:
        pd.DataFrame: Result from _analyze_jobs_byns_ondf.
    """

    df = data_repo.get_data(identifier)
    return _analyze_jobs_byns_ondf(df)

def analyze_cpu_only_jobs_byns(identifier: GrafanaIdentifier, data_repo: DataRepository):
    """
    Unpack the CPU Grafana DataFrame from the DataRepository and perform _analyze_jobs_byns_ondf on 
        it. Separate from the standard implementation of analyze_jobs_byns because we have to find
        the corresponding GPU DataFrame and use the uids from that as blacklisted uids.
    Maintains the sentiment that if the job exists in both CPU and GPU DataFrames it is NOT
        considered a CPU only job.
    
    Args:
        identifier (GrafanaIdentifier): The identifier for the Grafana DataFrame.
    Returns:
        pd.DataFrame: Result from _analyze_jobs_byns_ondf.
    """

    # We have to locate the corresponding GPU data block
    # The UIDs in the GPU will be used to clear blacklisted IDs
    gpu_identifier = GrafanaIdentifier(identifier.start_ts, identifier.end_ts, "gpu", identifier.query_cfg)
    if(not data_repo.contains(gpu_identifier)):
        raise Exception("Failed to analyze cpu only jobs, the corresponding gpu data_block could not be found.")
    
    gpu_df = data_repo.get_data(gpu_identifier)
    if(has_time_column(gpu_df)):
        gpu_df = clear_time_column(gpu_df)

    gpu_greater_than_zero_cols = [col for col in gpu_df.columns if gpu_df[col].sum() > 0]
    gpu_df = gpu_df[gpu_greater_than_zero_cols].fillna(0)
    gpu_uuid = set(gpu_df.columns.str.extract(r'uid="([^"]+)"')[0].dropna())

    # Unpack cpu dataframe
    df = data_repo.get_data(identifier)

    return _analyze_jobs_byns_ondf(df, gpu_uuid, True)

def _analyze_jobs_byns_ondf(df, blacklisted_uuids=None, strip_cols_0=True):
    """
    Analyze jobs by namespace. Calculates the unique amount of uids per namespace and sums them.

    Args:
        df (pd.DataFrame): The Grafana DataFrame to analyze
        blacklisted_uuids (list[str]): The blacklisted uuids to exclude from the job count.
        strip_cols_0 (bool): boolean to strip columns that total 0.
    Returns:
        pd.DataFrame: The result DataFrame with columns [Namespace, Count].    
    """

    # Preprocessing steps, clear time column, duplicate uids, and blacklisted uids.
    if(has_time_column(df)):
        df = clear_time_column(df)

    df = clear_duplicate_uids(df)
    if(blacklisted_uuids is not None):
        df = clear_blacklisted_uids(df, blacklisted_uuids)

    df = df.fillna(0)

    # If we want to strip the columns with 0 total values
    if(strip_cols_0):
        greater_than_zero_columns = [col for col in df.columns if df[col].sum() > 0]

        df = df[greater_than_zero_columns].fillna(0)

    #extract all the namespaces and use them as columns, group and summing the values 
    columns = df.columns.str.extract(r'namespace="([^"]+)"')[0]

    namespace_counts_sorted = pd.DataFrame(columns.value_counts().sort_values(ascending=False)).reset_index()
    namespace_counts_sorted.columns = ["Namespace", "Count"]

    return namespace_counts_sorted

def analyze_jobs_total(identifier, data_repo: DataRepository):
    """
    Unpack the jobs analysis DataFrame from the DataRepository and sum the Count column.

    Args:
        identifier (AnalysisIdentifier): The identifier for the previously computed jobs by
            namespace analysis.
    Returns:
        int: The sum of the count column.
    """

    df = data_repo.get_data(identifier)
    return df['Count'].sum()

def analyze_all_jobs_total(cpu_identifier: AnalysisIdentifier, data_repo: DataRepository):
    """
    Unpack the total jobs analysis for both the CPU and GPU DataFrame from the DataRepository and 
        add them together.

    Args:
        cpu_identifier (AnalysisIdentifier): The identifier for the previously computed jobs total
            analysis.
    Returns:
        int: The sum of the count column.
    """

    src_id = cpu_identifier.find_base()

    cpu_jobs_tot = data_repo.get_data(cpu_identifier)
    gpu_jobs_tot = data_repo.get_data(AnalysisIdentifier(AnalysisIdentifier(GrafanaIdentifier(src_id.start_ts, src_id.end_ts, "gpu", src_id.query_cfg), "gpujobs"), "gpujobstotal"))

    return cpu_jobs_tot + gpu_jobs_tot