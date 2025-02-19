import pandas as pd
import numpy as np
import gc

def preprocess_societe(df):
    """Prétraite les données sociétés avec optimisation mémoire"""
    try:
        if df.empty:
            return df
            
        if 'date_creation_def' in df.columns:
            df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
            # Utilise fillna pour éviter les erreurs de conversion NA
            df["annee_creation"] = df["date_creation_def"].dt.year.fillna(-1).astype('int32')
            
        # Optimisation des types de données
        if 'Activité principale' in df.columns:
            df['Activité principale'] = df['Activité principale'].astype('category')
        if 'Effectif_def' in df.columns:
            df['Effectif_def'] = df['Effectif_def'].astype('category')
            
        gc.collect()
        return df
    except Exception as e:
        print(f"Erreur dans preprocess_societe: {str(e)}")
        return df

def clean_montant(x):
    """Nettoie et convertit les montants en float32"""
    try:
        if pd.isna(x):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        
        # Gestion des listes de montants
        if '[' in str(x) or ',' in str(x):
            # Prend le premier montant si c'est une liste
            x = str(x).strip('[]').split(',')[0]
        
        return float(x.strip())
    except:
        return 0.0

def preprocess_financements(df):
    """Prétraite les données financements avec optimisation mémoire"""
    try:
        if df.empty:
            return df
            
        if 'Date dernier financement' in df.columns:
            df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
            df['Année'] = df['Date dernier financement'].dt.year.fillna(-1).astype('int32')
        
        if 'Montant_def' in df.columns:
            df['Montant_def'] = df['Montant_def'].apply(clean_montant).astype('float32')
        
        if 'Série' in df.columns:
            df['Série'] = df['Série'].astype('category')
            
        gc.collect()
        return df
    except Exception as e:
        print(f"Erreur dans preprocess_financements: {str(e)}")
        return df

def filter_societe(df, sector, effectif, year_range, year_col="annee_creation"):
    """Filtre un DataFrame de sociétés avec optimisation mémoire"""
    try:
        if df.empty:
            return df
            
        mask = pd.Series(True, index=df.index)
        
        if sector:
            mask &= df["Activité principale"].isin(sector)
        if effectif:
            mask &= df["Effectif_def"].isin(effectif)
        if year_range:
            mask &= df[year_col].between(year_range[0], year_range[1])
        
        filtered_df = df[mask]
        gc.collect()
        return filtered_df
    except Exception as e:
        print(f"Erreur dans filter_societe: {str(e)}")
        return df