import os
import pandas as pd
import shutil
from tqdm import tqdm
import yaml

from plugins.rci_plugins.promql.query_ingest import run, verify_query_config, DataFramePullException
from plugins.rci_plugins.rci_identifiers import GrafanaIdentifier
from src.data.data_repository import DataRepository
from src.data.timeline import Timeline
from src.plugin_mgmt.plugins import IngestPlugin
from src.program_data import ProgramData
from src.parameter_utils import ConfigurationException
from src.utils.timeutils import get_range_printable

class PromQLIngestController(IngestPlugin):
    """ The PromQLIngestController takes in a query configuration specifying: status query where we
            have the status of the pod, and a values query specifying the compute hours. The status 
            query is used as a filter for the values query, only taking values that show active in 
            status.
    """
    
    def verify_config_section(self, config_section) -> bool:
        """ The PromQLIngestController's config section expects a query-cfgs section with a list of 
                query configs to pull. 
        """

        def check_sec_exists(section):
            if(section not in config_section):
                raise ConfigurationException(f"The section \"{section}\" is not in the config section.")

        check_sec_exists("query-cfgs")

        if(not isinstance(config_section["query-cfgs"], list)):
            raise ConfigurationException("The query_cfgs section is not in the form of a list. PromQLIngestController query-cfgs section expects a list of names for query configs stored in the plugin_dir/ingest_configs/ directory.")

        for cfg_name in config_section["query-cfgs"]:
            load_query_config(cfg_name=cfg_name, verify=True)

        return True

    def ingest(self, prog_data: ProgramData, config_section: dict) -> DataRepository:
        
        data_repo = DataRepository()

        # Select between main periods and sub periods
        period_list = prog_data.timeline.periods
        if("main-periods" in config_section and config_section["main-periods"] is True):
            period_list = prog_data.timeline.main_periods

        # A dictionary of loaded cfg_name->config dictionary for flyweight pattern
        loaded_cfgs = {cfg_name: load_query_config(cfg_name) for cfg_name in config_section["query-cfgs"]}
        # A list of (cfg_name, type, period) tuples to pull from Prometheus
        pull_schedule = []
        cached_dfs = 0

        for cfg_name in config_section["query-cfgs"]:
            for type in loaded_cfgs[cfg_name]["yieldstypes"]:
                for period in period_list:

                    if(self.has_cached(loaded_cfgs[cfg_name], type, period)):
                        cached_df = self.get_cached(cfg_name, type, period)
                        identifier = GrafanaIdentifier(period[0], period[1], type, cfg_name)
                        data_repo.add(identifier, cached_df)
                        cached_dfs += 1
                    else:
                        pull_schedule.append((cfg_name, type, period))

        print(f"PromQL: Using {cached_dfs} cached DataFrames, pulling {len(pull_schedule)} new DataFrames from Prometheus.")

        if(len(pull_schedule) > 0):
            cached_status_dfs = {}
    
            pbar = tqdm(pull_schedule, unit="query")
            for pull_data in pbar:
                cfg_name, type, period = pull_data
                cfg = loaded_cfgs[cfg_name]

                pbar.set_description(f"{cfg_name} {type}@{get_range_printable(period[0], period[1], cfg["step"])}")

                try:
                    pulled_df = run(cfg, type, period, cached_status_dfs)
                except DataFramePullException as e:
                    tqdm.write(str(e))
                    continue

                identifier = GrafanaIdentifier(period[0], period[1], type, cfg_name)
                data_repo.add(identifier, pulled_df)

                self.add_cached(loaded_cfgs[cfg_name], type, period, pulled_df)

        data_repo = stitch(timeline=prog_data.timeline, data_repo=data_repo)

        print(f"PromQL: Done.")

        return data_repo
    
    def has_cached(self, cfg, type, period):
        """
        Check if we have the DataFrame cached for this configuration. It's important that the
            configuration matches the configuration saved in the cache, this is so we can avoid
            pulling inaccurate data if anything in the new config changes.
        
        Arguments:
            cfg (dict): The full query configuration dictionary.
            type (str): The type being pulled.
            period (tuple): The (start_ts, end_ts) period being pulled.
        """
        cache_root = os.path.join(CACHE_LOCATION, cfg["cfg_name"])
        if(not os.path.isdir(cache_root)):
            return False
        
        cached_cfg = os.path.join(cache_root, "config.yaml")
        if(not os.path.exists(cached_cfg)):
            return False

        with open(cached_cfg, "r") as file:
            cached = yaml.safe_load(file)

        # If the provided config doesn't match the cached config, delete cache so it can be
        #   rebuilt with the new config.
        if(cfg != cached):
            shutil.rmtree(cache_root)
            return False

        return os.path.exists(get_cache_path(cfg["cfg_name"], type, period))

    def get_cached(self, cfg_name, type, period):
        cache_path = get_cache_path(cfg_name, type, period)
        if(not os.path.exists(cache_path)):
            raise Exception(f"Failed to find cached DataFrame at expected location: {cache_path}")

        return pd.read_csv(cache_path)

    def add_cached(self, cfg, type, period, df: pd.DataFrame):
        cfg_name = cfg["cfg_name"]
        cache_root = os.path.join(CACHE_LOCATION, cfg_name)

        os.makedirs(cache_root, exist_ok=True)

        cached_cfg = os.path.join(cache_root, "config.yaml")
        with open(cached_cfg, "w") as file:
            yaml.safe_dump(cfg, file)

        cache_dir_path = get_cache_dir(cfg_name, type)
        os.makedirs(cache_dir_path, exist_ok=True)

        cache_path = get_cache_path(cfg_name, type, period)
        df.to_csv(cache_path, index=False)

def load_query_config(cfg_name, verify=False):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(dir_path, "ingest_configs", f"{cfg_name}.yaml")
    if(not os.path.exists(file_path)):
        excmsg = f"Failed to find ingest config named \"{cfg_name}\" was expecting to find the file at: {file_path}"
        if(verify):
            raise ConfigurationException(excmsg)
        else:
            raise Exception(excmsg)
    
    with open(file_path, "r") as file:
        query_cfg = yaml.safe_load(file)
        query_cfg["cfg_name"] = cfg_name

    if(verify):
        verify_query_config(query_cfg)

    return query_cfg

CACHE_LOCATION = "./io/cached_dfs"

def get_cache_dir(cfg_name, type):
    return os.path.join(CACHE_LOCATION, cfg_name, type)

def get_cache_path(cfg_name, type, period):
    cache_root = get_cache_dir(cfg_name, type)
    start_ts, end_ts = period
    file_name = f"{start_ts}-{end_ts}.csv"
    return os.path.join(cache_root, file_name)

def stitch(timeline: Timeline, data_repo: DataRepository):
    """
    Stitch multiple periods of identifiers together. For example, if the data is broken down into
        week-long periods, stitch will join together the weeks into months.

    Args:
        data_repo (DataRepository): The input repository, contains GrafanaIdentifiers to
            be transformed.
    
    Returns:
        DataRepository: The output DataRepository, contains SourceIdentifiers.
    """

    out_data_repo = DataRepository()

    def key_func(identifier: GrafanaIdentifier):
        return (identifier.query_cfg, identifier.type)

    buckets = {}

    # Sort identifiers into buckets by key function
    # Data in each bucket can be ordered by time and stitched together
    for identifier in data_repo.get_ids():
        key = key_func(identifier)
        if(key not in buckets):
            buckets[key] = []

        buckets[key].append(identifier)

    for bucket in buckets.keys():

        identifiers = buckets[bucket]
        if(len(identifiers) == 0):
            continue

        df = pd.DataFrame()
        # Stores a list of identifiers for this specific dataframe
        df_ids = [] 
        # Store the current main period we're stitching, this is used to detect when we're done
        #   and to move on to the next main period.
        timeline_idx = 0

        # Store the current data frame
        def store_df():
            nonlocal df, df_ids

            new_identifier = GrafanaIdentifier(df_ids[0].start_ts, df_ids[-1].end_ts, df_ids[0].type, df_ids[0].query_cfg)
            out_data_repo.add(new_identifier, df)

        def reset_df():
            nonlocal df, df_ids

            df = pd.DataFrame()
            df_ids = []

        identifiers.sort(key=lambda id: id.start_ts)
        for identifier in identifiers:

            current_timeline_period = timeline.main_periods[timeline_idx]
            if(identifier.start_ts > current_timeline_period[1]):
                raise Exception(f"Failed to stitch! Identifier {identifier} passed it's main period target ending point. Is there an identifier with an end timestamp that matches end timestamp for a main-period?")

            df_toadd = data_repo.get_data(identifier)
            df = pd.concat([df, df_toadd], ignore_index=True, sort=False)
            df_ids.append(identifier)

            if(identifier.end_ts == current_timeline_period[1]):
                store_df()
                reset_df()
                timeline_idx += 1

        if(timeline_idx != len(timeline.main_periods)):
            raise Exception(f"Failed to stitch! Not all main periods were stitched, stopped at timeline index {timeline_idx} out of {len(timeline.main_periods)}")

    return out_data_repo