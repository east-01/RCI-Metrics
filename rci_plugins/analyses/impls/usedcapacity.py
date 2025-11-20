from src.data.data_repository import DataRepository

def analyze_usedcapacity(identifier, data_repo: DataRepository):
    df = data_repo.get_data(identifier)
    value = df.iloc[:, 1].dropna().iloc[-1]
    avg_tb = value / 1_000_000_000_000

    # Average terabytes divided by 3 since the cluster is TRIPLE replicated.
    # Dividing by three shows the amount of truly used data
    avg_tb /= 3
    
    return avg_tb