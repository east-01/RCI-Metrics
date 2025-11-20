from dataclasses import dataclass
import math

from src.utils.timeutils import get_range_printable, break_period_into_months
from plugins.rci_plugins.promql.settings import settings
from src.data.timeline import Timeline

@dataclass(frozen=True)
class QueryData():
    """
    Information for a query, including the query url, the target type, and the period.
    """
    query_url: str
    query_name: str
    type: str
    start_ts: int
    end_ts: int

    def __str__(self) -> str:
        return f"{self.query_name} {self.type.upper()} {get_range_printable(self.start_ts, self.end_ts)}"

def build_query_url(config, query_name, type, period):
    query_string = config["queries"][query_name]
    type_string_identifier = settings['type_string_identifier']
    
    if(type is not None and type_string_identifier in query_string):
        type_strings = settings['type_strings']
        type_string = "|".join(type_strings[type])
        query_string = query_string.replace(type_string_identifier, type_string)

    return build_url(
        config["base_url"], 
        {
            "start": period[0],
            "end": period[1],
            "step": config["step"],
            "query": query_string
        }
    )

def build_url(base, url_options = {}):
    """
    Build a URL using a base url and additional options.
    Example: The URL https://google.com and the associated options { testopt: "testval" } will
      yield: https://google.com?testopt=testval
    """
    url = base

    if(len(url_options.keys()) > 0):
        url += '?'
        option_pairs = []
        for option_key in url_options:
            option_pairs.append(option_key + "=" + str(url_options[option_key]))
        url += "&".join(option_pairs)

    return url        