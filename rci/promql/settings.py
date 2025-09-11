settings = {
    "type_options": ["cpu", "gpu"],
    # A dictionary mapping a type option to the type that it appears as in the query
    "type_strings": {
        "cpu": "cpu",
        "gpu": "nvidia_com_gpu"
    },
    # The string in the query to be replaced with whatever type of data we're retrieving
    "type_string_identifier": "%TYPE_STRING%"#,
    # Analysis options, the types are the required types to perform the analysis.
    # Methods are filled out by analysis.py on the analysis() call
    # Requirements are fulfilled in the analysis() call
    # "analysis_settings": {
    #     "cpuhours": {
    #         "filter": filter_source_type("cpu"),
    #         "types": ["cpu"],
    #         "requires": [],
    #         "vis_options": {
    #             "type": "horizontalbar",
    #             "title": "Total CPU Hours by Namespace from %MONTH%",
    #             "subtext": "Total CPU Hours: %TOTCPUHRS%",
    #             "color": "skyblue",
    #             "variables": {
    #                 "TOTCPUHRS": "cpuhourstotal"
    #             }
    #         }
    #     },
    #     "cpuhourstotal": {
    #         "filter": filter_analyis_type("cpuhours"),
    #         "types": ["cpu"],
    #         "requires": ["cpuhours", "cpuhoursavailable"]
    #     },
    #     "cpuhoursavailable": {
    #         "filter": filter_source_type("cpu"),
    #         "types": [],
    #         "requires": []
    #     },
    #     "cpujobs": {
    #         "filter": filter_source_type("cpu"),
    #         "types": ["cpu", "gpu"],
    #         "requires": ["gpujobs"],
    #         "vis_options": {
    #             "type": "horizontalbar",
    #             "title": "Total CPU Jobs by Namespace from %MONTH%",
    #             "subtext": "Total CPU Jobs: %TOTCPUJOBS%",
    #             "color": "skyblue",
    #             "variables": {
    #                 "TOTCPUJOBS": "cpujobstotal"
    #             }
    #         }
    #     },
    #     "cpujobstotal": {
    #         "filter": filter_analyis_type("cpujobs"),
    #         "types": ["cpu", "gpu"],
    #         "requires": ["cpujobs"]
    #     },
    #     "gpuhours": {
    #         "filter": filter_source_type("gpu"),
    #         "types": ["gpu"],
    #         "requires": [],
    #         "vis_options": {
    #             "type": "horizontalbar",
    #             "title": "Total GPU Hours by Namespace from %MONTH%",
    #             "subtext": "Total GPU Hours: %TOTGPUHRS%",
    #             "color": "orange",
    #             "variables": {
    #                 "TOTGPUHRS": "gpuhourstotal"
    #             }
    #         }
    #     },
    #     "gpuhourstotal": {
    #         "filter": filter_analyis_type("gpuhours"),
    #         "types": ["gpu"],
    #         "requires": ["gpuhours", "gpuhoursavailable"]
    #     },
    #     "gpuhoursavailable": {
    #         "filter": filter_source_type("gpu"),
    #         "types": [],
    #         "requires": []
    #     },
    #     "gpujobs": {
    #         "filter": filter_source_type("gpu"),
    #         "types": ["gpu"],
    #         "requires": [],
    #         "vis_options": {
    #             "type": "horizontalbar",
    #             "title": "Total GPU Jobs by Namespace from %MONTH%",
    #             "subtext": "Total GPU Jobs: %TOTGPUJOBS%",
    #             "color": "orange",
    #             "variables": {
    #                 "TOTGPUJOBS": "gpujobstotal"
    #             }
    #         }
    #     },
    #     "gpujobstotal": {
    #         "filter": filter_analyis_type("gpujobs"),
    #         "types": ["gpu"],
    #         "requires": ["gpujobs"]
    #     },
    #     "jobstotal": {
    #         "filter": filter_analyis_type("cpujobstotal"),
    #         "types": ["cpu", "gpu"],
    #         "requires": ["cpujobstotal", "gpujobstotal"]
    #     },
    #     "cvgpuhours": {
    #         "types": ["cpu", "gpu"],
    #         "requires": ["cpuhourstotal", "gpuhourstotal"],
    #         "vis_options": {
    #             "type": "timeseries",
    #             "title": "CPU and GPU hours by month",
    #             "color": {
    #                 "cpuhourstotal": "red",
    #                 "gpuhourstotal": "blue"
    #             }
    #         }
    #     },
    #     "cvgpujobs": {
    #         "types": ["cpu", "gpu"],
    #         "requires": ["cpujobstotal", "gpujobstotal"],
    #         "vis_options": {
    #             "type": "timeseries",
    #             "title": "CPU and GPU jobs by month",
    #             "color": {
    #                 "cpujobstotal": "red",
    #                 "gpujobstotal": "blue"
    #             }
    #         }
    #     },
    #     "utilization": {
    #         "types": ["cpu", "gpu"],
    #         "requires": ["cpuhourstotal", "cpuhoursavailable", "gpuhourstotal", "gpuhoursavailable"],
    #         "vis_options": None
    #     }
    # },
    # # Information about hardware configurations for each node.
    # "node_infos": {
    #     "rci-tide-cpu": {
    #         "cpu": 64,
    #         "gpu": 4,
    #         "node_cnt": 6
    #     },
    #     "rci-tide-gpu": { # Targets L40 GPUs
    #         "cpu": 24,
    #         "gpu": 4,
    #         "node_cnt": 17
    #     },
    #     "rci-nrp-gpu": { # Targets A100 GPUs
    #         "cpu": 64,
    #         "gpu": 4,
    #         "node_cnt": 8
    #     }
    # }
}