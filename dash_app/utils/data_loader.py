import os
import pandas as pd
from functools import lru_cache
import gc

ESSENTIAL_COLUMNS = {
    'societes': ['entreprise_id', 'nom', 'date_creation_def', 'Activité principale', 
                 'Effectif_def', 'latitude', 'longitude', 'adresse_def', 'description',
                 'logo', 'site_web_def', 'SIRET', 'market', 'Sous-Catégorie', 'mots_cles_def'],
    'financements': ['entreprise_id', 'Montant_def', 'Date dernier financement', 'Série'],
    'personnes': ['entreprise_id']
}

DTYPE_MAPPINGS = {
    'entreprise_id': 'str',
    'nom': 'category',
    'Activité principale': 'category',
    'Effectif_def': 'category',
    'adresse_def': 'category',
    'market': 'category',
    'Série': 'category',
    'SIRET': 'str',
    'latitude': 'float32',
    'longitude': 'float32'
}

def clean_montant(x):
    """Nettoie et convertit les montants en float32"""
    try:
        if pd.isna(x):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        
        # Gestion des listes de montants
        if '[' in str(x) or ',' in str(x):
            x = str(x).strip('[]').split(',')[0]
        
        return float(x.strip())
    except:
        return 0.0

@lru_cache(maxsize=3)
def get_dataframe(filename, usecols=None):
    """Charge un DataFrame depuis un fichier CSV avec optimisations mémoire"""
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../assets")
    file_type = filename.split('.')[0]
    
    try:
        # Vérifie les colonnes disponibles
        available_cols = pd.read_csv(os.path.join(base_path, filename), nrows=0).columns
        
        # Utilise uniquement les colonnes essentielles
        if usecols is None and file_type in ESSENTIAL_COLUMNS:
            usecols = [col for col in ESSENTIAL_COLUMNS[file_type] if col in available_cols]
        elif usecols:
            usecols = [col for col in usecols if col in available_cols]
        
        # Prépare le mapping des types
        dtype_dict = {col: DTYPE_MAPPINGS[col] 
                     for col in usecols 
                     if col in DTYPE_MAPPINGS}
        
        # Identifie les colonnes de dates
        date_cols = [col for col in usecols 
                    if 'date' in col.lower() or 
                       'creation' in col.lower() or 
                       'financement' in col.lower()]
        
        # Lecture optimisée avec chunking
        chunks = []
        chunk_size = 10000  # Taille de chunk réduite
        
        for chunk in pd.read_csv(
            os.path.join(base_path, filename),
            usecols=usecols,
            dtype=dtype_dict,
            parse_dates=date_cols,
            chunksize=chunk_size,
            low_memory=False,
            na_values=['', 'nan', 'NaN', 'NA', 'null'],
            encoding='utf-8'
        ):
            # Traitement spécial pour les montants
            if 'Montant_def' in chunk.columns:
                chunk['Montant_def'] = chunk['Montant_def'].apply(clean_montant).astype('float32')
            
            # Optimisations par chunk
            for col in chunk.select_dtypes(include=['object']):
                if col not in date_cols and col not in ['entreprise_id', 'SIRET']:
                    chunk[col] = chunk[col].astype('category')
            
            chunks.append(chunk)
            gc.collect()
        
        # Combine les chunks
        df = pd.concat(chunks, ignore_index=True)
        
        # Optimisations post-lecture
        for col in df.select_dtypes(include=['category']):
            df[col] = df[col].cat.remove_unused_categories()
        
        return df
        
    except Exception as e:
        print(f"Erreur lors du chargement de {filename}: {str(e)}")
        return pd.DataFrame(columns=['entreprise_id'])
    finally:
        gc.collect()