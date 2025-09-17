def get_node_prefix(node_name: str):
    """
    Get the node prefix that will be usable in settings.py.
    Makes assumptions about node names:
        - The node name ends with .sdsu.edu
        - The node's number ends with -XX
    
    Args:
        node_name (str): The name of the node as supplied by prometheus
    Returns:
        str: The node prefix usable in settings.py
    """

    if(not node_name.endswith(".sdsu.edu")):
        raise Exception(f"Failed to get node prefix from node name \"{node_name}\" it does not end with .sdsu.edu.")
    
    node_name = node_name.replace(".sdsu.edu", "")
    node_name = "-".join(node_name.split("-")[:-1])

    return node_name