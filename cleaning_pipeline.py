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

    logger.info("Données chargées")

    return df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft


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
def clean_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft):
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
def merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft):
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

    logger.info("Fusion terminée.")

    # Fusion des mots clés
    mots_cles_cols = ['mots_clé','mots_cles','mots_cles_z','mots_cles_y','mots_cles_a','mots_cles_t','mots_clé_2', 'mots_clé_3', 'mots_clé_4', 'mot_clé_5']
    
    # Fusionner les colonnes directement en une seule colonne "mots_cles"
    merged_df["mots_cles"] = (
    merged_df[mots_cles_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    
    logger.info("Mots clés fusionnés")

    #Suppression des colonnes
    merged_df.drop(columns=mots_cles_cols, inplace=True)
    merged_df.drop(columns=['site_web_y','site_web_z','site_web_z','site_web_u','site_web_t'], inplace=True)
    merged_df.drop(columns=['Field_1','Field_2','Field_3'], inplace=True)
    merged_df.drop(columns=['city_x','city_y','Adresse','Date de création_y','emplacement','fundraising','address','Denomination légale'], inplace=True)
    merged_df = merged_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Supprimer les espaces en début/fin
    merged_df = merged_df.applymap(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)  # Remplacer les espaces multiples
    
    logger.info("Colonnes supprimées")

    return merged_df


@task
def save_data(df):
    """Sauvegarde la merged_df en CSV"""
    df.to_csv('merged_df.csv', index=False)
    logger.info(f"Données sauvegardées")


@flow
def data_pipeline():
    """Lancement de toutes les task de netoyage"""

    # Chargement des données
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft = load_data()
    # Déballage de df_mina
    df_mina = debal_minalogic(df_mina)
    # Nettoyage des DataFrames
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft = clean_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft)
    # Fusion des DataFrames
    merged_df = merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren, df_pepites, df_ft)
    # Sauvegarde
    save_data(merged_df)


if __name__ == "__main__":
    data_pipeline()
