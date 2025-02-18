import os
import pandas as pd

def get_dataframe(filename):
    """Charge un DataFrame depuis un fichier CSV"""
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../assets")
    return pd.read_csv(os.path.join(base_path, filename), low_memory=False)
