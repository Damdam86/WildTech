import pandas as pd

def preprocess_societe(df):
    df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
    df["annee_creation"] = df["date_creation_def"].dt.year
    return df

def preprocess_financements(df):
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0)
    return df

def filter_societe(df, sector, effectif, year_range, year_col="annee_creation"):
    """Filtre un DataFrame de sociétés selon l'activité, l'effectif et la plage d'années."""
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[df[year_col].between(year_range[0], year_range[1])]
    return df
