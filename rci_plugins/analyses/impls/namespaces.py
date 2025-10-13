from plugins.rci_plugins.promql.grafana_df_cleaning import has_time_column, clear_time_column
from src.data.data_repository import DataRepository

def analyze_uniquens(identifier, data_repo: DataRepository):
    df = data_repo.get_data(identifier)

    if(has_time_column(df)):
        df = clear_time_column(df)

	# Extract namespaces from column names, excluding the first since that's the Time column
    namespaces = df.columns.str.extract(r'namespace="([^"]+)"')[0]
    namespaces = set(namespaces)

    return namespaces

def analyze_all_uniquens(identifiers, data_repo: DataRepository):

    unique_nss = set()

    for identifier in identifiers:
        preexisting = data_repo.get_data(identifier)
        unique_nss |= preexisting

    return unique_nss