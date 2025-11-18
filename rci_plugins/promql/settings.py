settings = {
    "type_options": ["cpu", "gpu", "storage"],
    # A dictionary mapping a type option to the type that it appears as in the query
    "type_strings": {
        "cpu": ["cpu"],
        "gpu": ["nvidia_com_gpu", "nvidia_com_a100"]
    },
    # The string in the query to be replaced with whatever type of data we're retrieving
    "type_string_identifier": "%TYPE_STRING%"
}