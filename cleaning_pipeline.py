import os
import pandas as pd
import ast
from prefect import flow, task
from prefect.logging import get_logger

logger = get_logger()

@task
def load_data():
    """ Charge les données depuis les fichiers JSON et CSV"""  
    df_bpi = pd.read_json(r'sources/bpifrance_startups_data2.json')
    df_tech = pd.read_json(r"sources/tech_fest_data.json")
    df_maddy = pd.read_json(r"sources/entreprises_data - maddyness.json")
    df_CESFR = pd.read_csv(r"sources/exposantsFRCES2025.csv")
    df_mina = pd.read_csv(r'sources/societes_minalogic.csv')
    df_viva = pd.read_csv(r'sources/partners_viva_tech.csv')
    df_siren = pd.read_csv(r'sources/enriched_company_data.csv')
    df_pepites = pd.read_csv(r'sources/startup_pepites_category_website.csv')
    df_ft = pd.read_csv(r'sources/df_french_tech.csv')
    df_keywords = pd.read_csv(r'sources/add_mot_cles.csv')



    logger.info("Données chargées")

    return df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft, df_keywords


@task
def debal_minalogic(df_mina):
    """Déballe les colonnes 'contact' et 'infos' dans df_mina"""
    def to_dict(x):
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except:
                return None
        return x

    df_mina['contact'] = df_mina['contact'].apply(to_dict)
    df_mina['infos'] = df_mina['infos'].apply(to_dict)

    df_mina['Adresse'] = df_mina['contact'].apply(lambda d: d.get('Adresse') if isinstance(d, dict) else None)
    df_mina['Contact'] = df_mina['contact'].apply(lambda d: d.get('Contact') if isinstance(d, dict) else None)
    df_mina['Date de création'] = df_mina['infos'].apply(lambda d: d.get('Date de création') if isinstance(d, dict) else None)
    df_mina['mots_cles_a'] = df_mina['infos'].apply(lambda d: d.get('Thématiques') if isinstance(d, dict) else None)
    df_mina['Marché'] = df_mina['infos'].apply(lambda d: d.get('Marchés') if isinstance(d, dict) else None)
    df_mina["Date d'adhésion"] = df_mina['infos'].apply(lambda d: d.get("Date d'adhésion") if isinstance(d, dict) else None)
    df_mina["Type d'organisme"] = df_mina['infos'].apply(lambda d: d.get("Type d'organisme") if isinstance(d, dict) else None)

    logger.info("Déballage terminé.")

    return df_mina


@task
def cleaning_data1(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft):
    """Nettoie les DataFrames"""
    rename_mappings = {
        'df_bpi': {'name': 'nom', 'hashtags': 'mots_cles_b', 'website': 'site_web'},
        'df_CESFR': {'Nom': 'nom', 'Description': 'description', 'Catégories': 'mots_cles_y', 
                     'Website': 'site_web_z', 'Adresse': 'adresse_z', 'Logo': 'logo'},
        'df_maddy': {'Nom': 'nom', 'Description': 'description', 'Site internet': 'site_web', 
                     'Logo': 'logo', 'Hashtags': 'mots_cles_z'},
        'df_mina': {'name': 'nom', 'Logo': 'logo'},
        'df_viva': {'name': 'nom', 'website': 'site_web_u', 'Logo': 'logo'},
        'df_pepites' : {'title':'nom', 'adress':'adresse', 'logo_url':'logo', 'website':'site_web_t', 'category':'mots_cles_t'}
    }

    for df_name, rename_map in rename_mappings.items():
        df = locals()[df_name]
        df.rename(columns=rename_map, inplace=True)

    logger.info("Renomage terminé")

    # Suppression des colonnes inutiles
    def col_drop(df, cols):
        existing_cols = [col for col in cols if col in df.columns]
        df.drop(columns=existing_cols, inplace=True, errors='ignore')

    col_drop(df_tech, ['social_links'])
    col_drop(df_bpi, ['total_funding'])
    col_drop(df_CESFR, ['Lien'])
    col_drop(df_maddy, ['Siège', 'Date de création'])
    col_drop(df_mina, ['url', "Date d'adhésion", "contact", "infos"])
    col_drop(df_viva, ["link", "looking_for", "development_level"])
    col_drop(df_siren, ["État administratif", "Catégorie entreprise", "Denomination usuelle"])
    col_drop(df_pepites, ["url"])

    logger.info("Suppréssion terminée")

    # Mise en majuscules des noms des sociétés
    for df in [df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_pepites]:
        if 'nom' in df.columns:
            df['nom'] = df['nom'].str.upper()

    logger.info("Nettoyage terminé.")

    return df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft


@task
def merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft, df_keywords):
    """Fusionne les DataFrames"""
    # Fusion df_bpi et df_tech
    merged_df = pd.merge(df_bpi, df_tech, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_maddy
    merged_df = pd.merge(merged_df, df_maddy, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_CESFR
    merged_df = pd.merge(merged_df, df_CESFR, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_mina
    merged_df = pd.merge(merged_df, df_mina, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_viva
    merged_df = pd.merge(merged_df, df_viva, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_siren
    merged_df = pd.merge(merged_df, df_siren, on=['nom'], how='outer')
    # Fusion de la merge avec df_pepites
    merged_df = pd.merge(merged_df, df_pepites, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_ft
    merged_df = pd.merge(merged_df, df_ft, on=['nom','description','logo'], how='outer')
    # Fusion de la merge avec df_ft
    merged_df = pd.merge(merged_df, df_keywords, on=['nom','description','logo'], how='outer')

    logger.info("Fusion terminée.")
    
    return merged_df

def flatten_address(address):
    """Nettoie et transforme les listes d'adresses en chaînes uniques."""
    if pd.isna(address):  # Vérifie si l'adresse est NaN
        return None

    if isinstance(address, str):
        try:
            address = ast.literal_eval(address)  # Convertit les chaînes de type liste en vraies listes
        except:
            pass  # Ignore les erreurs si ce n'est pas une liste encodée en str

    if isinstance(address, list):  # Vérifie si c'est une liste
        address = [a.strip() for a in address if isinstance(a, str) and a.strip()]  # Supprime les entrées vides et espaces
        return ", ".join(set(address)) if address else None  # Supprime les doublons et convertit en chaîne unique
    
    return address  # Retourne tel quel si ce n'est pas une liste

@task
def cleaning_data2(merged_df):    
    """Nettoie les DataFrames aprés merge"""

    # Fusion des mots clés
    mots_cles_cols = ['mots_clé','mots_cles_z','mots_cles_x','mots_cles_y','mots_cles_a','field','product_types','sectors','mots_cles_b','mots_cles_t','mots_clé_2', 'mots_clé_3', 'mots_clé_4', 'mot_clé_5','Marché']
    merged_df["mots_cles_def"] = (
    merged_df[mots_cles_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    
    logger.info("Mots clés fusionnés")

    # Fusion des sites internet
    site_web_cols = ['site_web', 'site_web_x','site_web_y','site_web_z','site_web_z','site_web_u','site_web_t']
    merged_df["site_web_def"] = (
    merged_df[site_web_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    logger.info("Site web fusionnés")

    # Fusion des adresses
    adresse_cols = ['adresse_z', 'adresse', 'city_x','city_y','Adresse','address']
    merged_df["adresse_def"] = (
    merged_df[adresse_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    logger.info("Adresses fusionnés")
    
    # Fusion des dates de création
    creation_cols = ['Date de création_x', 'date de création','Date de création_y']
    merged_df["date_creation_def"] = (
    merged_df[creation_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    logger.info("Date création fusionnés")

    # Nettoyage des adresses
    merged_df["adresse_def"] = merged_df["adresse_def"].apply(flatten_address)

    # 🔹 Suppression des valeurs incorrectes et des espaces inutiles
    merged_df["adresse_def"] = (
        merged_df["adresse_def"]
        .astype(str)  # Convertit en chaîne pour éviter les erreurs .str
        .replace("nan", pd.NA)  # Convertit 'nan' en NaN réel
        .replace("['']", pd.NA)  # Supprime les listes vides formatées comme string
        .str.replace(r'^[\'"\[]|[\'"\]]$', '', regex=True)  # Supprime quotes et crochets
        .str.strip()  # Supprime les espaces inutiles 
        )

    logger.info("Adresses nettoyées")

    #Suppression des colonnes
    merged_df.drop(columns=mots_cles_cols, inplace=True)
    merged_df.drop(columns=site_web_cols, inplace=True)
    merged_df.drop(columns=adresse_cols, inplace=True)
    merged_df.drop(columns=creation_cols, inplace=True)
    merged_df.drop(columns=['Field_1','Field_2','Field_3','tags','page_french_tech'], inplace=True)
    merged_df.drop(columns=['emplacement','fundraising','Denomination légale'], inplace=True)
    merged_df = merged_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Supprimer les espaces en début/fin
    merged_df = merged_df.applymap(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)  # Remplacer les espaces multiples
    
    logger.info("Colonnes supprimées")

    return merged_df

@task
def save_data(df):
    """Sauvegarde la merged_df en CSV"""
    df.to_csv('merged_df.csv', index=False)
    logger.info(f"Données sauvegardées")

@task
def create_database(merged_df):

    # Conversion des colonnes susceptibles de contenir des listes en tuples pour éviter des erreurs de hashage
    def make_hashable(x):
        return tuple(x) if isinstance(x, list) else x
    
    cols_to_convert = ['description', 'logo', "Type d'organisme", 'SIREN',
                       'Activité principale', 'Tranche effectifs', 'market',
                       'employees', 'mots_cles_def', 'site_web_def', 'adresse_def',
                       'date_creation_def']
    for col in cols_to_convert:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].apply(make_hashable)
    
    # Table société avec créaton ID
    societes = merged_df[['nom', 'description', 'logo', "Type d'organisme", 'SIREN', 
                            'Activité principale', 'Tranche effectifs', 'market', 
                            'employees', 'mots_cles_def', 'site_web_def', 'adresse_def', 
                            'date_creation_def']].drop_duplicates()
    societes.insert(0, "entreprise_id", range(1, len(societes) + 1))
    
    # Table des Personnes avec entreprise_id et création contact_id
    personnes = merged_df[['nom', 'Contact']].drop_duplicates()
    personnes = personnes.merge(societes[['nom', 'entreprise_id']], on='nom', how='left')
    personnes = personnes[['entreprise_id', 'Contact']]
    personnes.insert(0, "contact_id", range(1, len(personnes) + 1))
    
    # Table des Financements avec entreprise_id et création de financement_id
    financements = merged_df[['nom', 'Date dernier financement', 'Série', 'Montant', 
                                'valeur_entreprise', 'financement', 'dernier_financement']].drop_duplicates()
    financements = financements.merge(societes[['nom', 'entreprise_id']], on='nom', how='left')
    financements = financements[['entreprise_id', 'Date dernier financement', 'Série', 'Montant', 
                                   'valeur_entreprise', 'financement', 'dernier_financement']]
    financements.insert(0, "financement_id", range(1, len(financements) + 1))
    
    # Sauvegarde des datasets
    societes.to_csv("societes.csv", index=False)
    personnes.to_csv("personnes.csv", index=False)
    financements.to_csv("financements.csv", index=False)
    
    logger.info("Les trois datasets ont été créés.")

# Exemple d'appel de la fonction :
# create_database(merged_df)


@flow
def data_pipeline():
    """Lancement de toutes les task de netoyage"""

    # Chargement des données
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft, df_keywords = load_data()
    # Déballage de df_mina
    df_mina = debal_minalogic(df_mina)
    # Nettoyage des DataFrames 1 
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft = cleaning_data1(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft)
    # Fusion des DataFrames
    merged_df = merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft, df_keywords)
    # Nettoyage des DataFrames 2
    merged_df = cleaning_data2(merged_df)
    # Sauvegarde
    save_data(merged_df)
    # Création de la multibase de données
    create_database(merged_df)

if __name__ == "__main__":
    data_pipeline()
