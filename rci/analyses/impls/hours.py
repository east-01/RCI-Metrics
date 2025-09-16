# This code is repackaged from Tide2.ipynb in https://github.com/SDSU-Research-CI/rci-helpful-scripts
import pandas as pd

from plugins.rci.promql.grafana_df_cleaning import has_time_column, clear_time_column
from plugins.rci.rci_identifiers import AvailableHoursIdentifier
from src.data.data_repository import DataRepository
from src.data.identifier import *

def analyze_hours_byns(identifier, data_repo: DataRepository):
	"""
	Unpack the Grafana DataFrame from the DataRepository and perform _analyze_hours_byns_ondf on
		it.

	Args:
		identifier (SourceIdentifier): The identifier for the Grafana DataFrame.
	Returns:
		pd.DataFrame: Result from _analyze_hours_byns_ondf.
	"""

	df = data_repo.get_data(identifier)
	return _analyze_hours_byns_ondf(df)

def _analyze_hours_byns_ondf(df):
	"""
	Analyze hours by namespace.

	Args:
		df (pd.DataFrame): The Grafana DataFrame to analyze
	Returns:
		pd.DataFrame: The result DataFrame with columns [Namespace, Hours].    
	"""

	if(has_time_column(df)):
		df = clear_time_column(df)

	# Extract namespaces from column names, excluding the first since that's the Time column
	namespaces = df.columns.str.extract(r'namespace="([^"]+)"')[0]

	# Calculate the sum for each namespace
	namespace_totals = {}
	for namespace in namespaces.unique():
		namespace_df = df.filter(regex=f'namespace="{namespace}"', axis=1)

		namespace_total = namespace_df.sum(axis=1).sum()
		namespace_totals[namespace] = namespace_total

	namespace_totals_df = pd.DataFrame(list(namespace_totals.items()), columns=["Namespace", "Hours"])

	# Drop NA and 0 values
	namespace_totals_df.dropna(inplace=True)
	namespace_totals_df = namespace_totals_df[namespace_totals_df["Hours"] >= 0.001]

	# Sort the final DataFrame by hours
	namespace_totals_df.sort_values(by="Hours", ascending=False, inplace=True)

	return namespace_totals_df

def analyze_hours_total(identifier, data_repo: DataRepository):
	"""
	Unpack the analysis DataFrame from the DataRepository and sum the Hours column.

	Args:
		identifier (AnalysisIdentifier): The identifier for the previously computed hours by
			namespace analysis.
	Returns:
		float: The sum of the hours column.
	"""

	# Retrieve the corresponding analysis thats already been performed
	df = data_repo.get_data(identifier)
	total_hours = df['Hours'].sum()
	
	return total_hours

def verify_hours(identifier: AnalysisIdentifier, data_repo: DataRepository):
	"""
	Unpack the total hours float from the analysis, and check if it is not greater than the total
		hours available.

	Args:
		identifier (AnalysisIdentifier): The identifier for the previously computed hours by
			namespace analysis.
	Returns:
		float: The sum of the hours column.
	"""

	total_hours = data_repo.get_data(identifier)

	src_id = identifier.find_base()
	src_as_timestamp = TimeStampIdentifier(src_id.start_ts, src_id.end_ts)
	avail_hrs_analysis_id = AvailableHoursIdentifier(src_as_timestamp, src_id.type + "hoursavailable", src_id.type, "default")
	avail_hrs = data_repo.get_data(avail_hrs_analysis_id)

	# Ensure the hours we calculated doesn't exceed the maximum possible hours
	if(total_hours > avail_hrs):
		print(f"The total hours scheduled {total_hours} exceeds the maximum amount of resource hours available {avail_hrs}.")
	
	return total_hours <= avail_hrs