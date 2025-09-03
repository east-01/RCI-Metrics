# This code is repackaged from Tide2.ipynb in https://github.com/SDSU-Research-CI/rci-helpful-scripts
import pandas as pd

from src.data.data_repository import DataRepository
from plugins.promql.grafana_df_cleaning import has_time_column, clear_time_column, _extract_column_data
from plugins.promql.grafana_df_analyzer import get_period
from plugins.promql.settings import settings
from plugins.rci.rci_identifiers import GrafanaIdentifier, AvailableHoursIdentifier
from src.data.identifier import *
from src.utils.tideutils import *

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

	print("TODO: hours.py:76 add back in hours check after implementing hours available benchmarks")
	# src_id = identifier.find_base()
	# src_as_timestamp = TimeStampIdentifier(src_id.start_ts, src_id.end_ts)
	# print(src_id)
	# avail_hrs_analysis_id = AvailableHoursIdentifier(src_as_timestamp, src_id.type + "hoursavailable", src_id.type, "default")
	# avail_hrs = data_repo.get_data(avail_hrs_analysis_id)
	# print(avail_hrs)

	# # Ensure the hours we calculated doesn't exceed the maximum possible hours
	# if(total_hours > avail_hrs):
	# 	raise Exception(f"The total hours scheduled {total_hours} exceeds the maximum amount of resource hours available {avail_hrs}.")

	return total_hours

def analyze_available_hours(identifier: GrafanaIdentifier, data_repo: DataRepository):
	"""
	Unpack the Grafana DataFrame from the DataRepository and perform _analyze_available_hours_ondf
		on it.

	Args:
		identifier (SourceIdentifier): The identifier for the Grafana DataFrame.
	Returns:
		float: The result of _analyze_available_hours_ondf.
	"""
	
	df = data_repo.get_data(identifier)

	return _analyze_available_hours_ondf(df, identifier.type, identifier.start_ts, identifier.end_ts)

def _analyze_available_hours_ondf(df, df_type, start_ts, end_ts):
	"""
	Determine the amount of available compute hours for the specific type.

	Args:
		identifier (SourceIdentifier): The identifier for the Grafana DataFrame.
	Returns:
		float: The total amount of compute hours available.
	"""

	if(has_time_column(df)):
		df = clear_time_column(df)
		
	# Calculate hour amount
	total_hours_month = (end_ts-start_ts+1)/3600

	# Loop through each node name adding resource count * hours to the total	
	node_infos = settings["node_infos"]

	if(df_type=="cpu"):

		cpu_info = node_infos["rci-tide-cpu"]
		cpu_cpus_avail = cpu_info["node_cnt"] * (cpu_info["cpu"]-2)

		tide_gpu_info = node_infos["rci-tide-gpu"]
		gpu_cpus_avail = tide_gpu_info["node_cnt"] * (tide_gpu_info["cpu"]-2)

		cpus_avail = cpu_cpus_avail + gpu_cpus_avail

		return cpus_avail * total_hours_month

	elif(df_type=="gpu"):

		tide_gpu_info = node_infos["rci-tide-gpu"]
		gpus_avail = tide_gpu_info["node_cnt"] * tide_gpu_info["gpu"]
		
		return gpus_avail * total_hours_month

	# total_resource_hours = node_infos[]

	# unout = list(unique_nodes)
	# unout.sort()
	# print("\n".join(unout))

	# for node_name in unique_nodes:
	# 	# Get prefix and ensure it exists in the infos dict
	# 	node_prefix = get_node_prefix(node_name)
	# 	if(node_prefix not in node_infos):
	# 		raise Exception(f"Node prefix \"{node_prefix}\" is not in settings.py node_infos.")

	# 	node_info = node_infos[node_prefix]
	# 	resources = node_info[df_type]

	# 	total_resource_hours += resources * total_hours_month

	# return total_resource_hours

