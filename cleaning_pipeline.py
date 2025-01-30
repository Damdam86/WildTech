import os
import pandas as pd
import ast
from prefect import flow, task
from prefect.logging import get_logger

logger = get_logger("data_pipeline")

@task
def load_data():
    """📥 Charge les données depuis les fichiers JSON et CSV"""  
    logger.info("📥 Chargement des données...")

    df_bpi = pd.read_json(r'sources/bpifrance_startups_data2.json')
    df_tech = pd.read_json(r"sources/tech_fest_data.json")
    df_maddy = pd.read_json(r"sources/entreprises_data - maddyness.json")
    df_CESFR = pd.read_csv(r"sources/exposantsFRCES2025.csv")
    df_mina = pd.read_csv(r'sources/societes_minalogic.csv')
    df_viva = pd.read_csv(r'sources/partners_viva_tech.csv')
    df_siren = pd.read_csv(r'sources/enriched_company_data.csv')

    logger.info("✅ Données chargées avec succès.")

    return df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren


@task
def unpack_minalogic(df_mina):
    """🔄 Déballe les colonnes 'contact' et 'infos' dans df_mina"""
    logger.info("🔄 Déballage des données Minalogic...")

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

    logger.info("✅ Déballage terminé.")

    return df_mina


@task
def clean_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren):
    """🔄 Nettoie et normalise les DataFrames"""
    logger.info("🔄 Nettoyage et normalisation des données...")

    rename_mappings = {
        'df_bpi': {'name': 'nom', 'hashtags': 'mots_cles_b', 'website': 'site_web'},
        'df_CESFR': {'Nom': 'nom', 'Description': 'description', 'Catégories': 'mots_cles_y', 
                     'Website': 'site_web_z', 'Adresse': 'adresse_z', 'Logo': 'logo'},
        'df_maddy': {'Nom': 'nom', 'Description': 'description', 'Site internet': 'site_web', 
                     'Logo': 'logo', 'Hashtags': 'mots_cles_z'},
        'df_mina': {'name': 'nom', 'Logo': 'logo'},
        'df_viva': {'name': 'nom', 'website': 'site_web_u', 'Logo': 'logo'}
    }

    for df_name, rename_map in rename_mappings.items():
        df = locals()[df_name]
        df.rename(columns=rename_map, inplace=True)

    # Suppression des colonnes inutiles
    def safe_drop(df, cols):
        existing_cols = [col for col in cols if col in df.columns]
        df.drop(columns=existing_cols, inplace=True, errors='ignore')

    safe_drop(df_tech, ['social_links'])
    safe_drop(df_bpi, ['total_funding'])
    safe_drop(df_CESFR, ['Lien'])
    safe_drop(df_maddy, ['Siège', 'Date de création'])
    safe_drop(df_mina, ['url', "Date d'adhésion", "contact", "infos"])
    safe_drop(df_viva, ["link", "looking_for", "development_level"])
    safe_drop(df_siren, ["État administratif", "Catégorie entreprise", "Denomination usuelle"])

    # Mise en majuscules
    for df in [df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva]:
        if 'nom' in df.columns:
            df['nom'] = df['nom'].str.upper()

    logger.info("✅ Nettoyage terminé.")

    return df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren


@task
def merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren):
    """🔄 Fusionne les DataFrames"""
    logger.info("🔄 Fusion des DataFrames...")

    merged_df = df_bpi.merge(df_tech, on=['nom','description','logo'], how='outer')\
                      .merge(df_maddy, on=['nom','description','logo'], how='outer')\
                      .merge(df_CESFR, on=['nom','description','logo'], how='outer')\
                      .merge(df_mina, on=['nom','description','logo'], how='outer')\
                      .merge(df_viva, on=['nom','description','logo'], how='outer')\
                      .merge(df_siren, on=['nom'], how='outer')

    logger.info("✅ Fusion terminée.")

    return merged_df


@task
def save_data(df):
    """💾 Sauvegarde le DataFrame final en CSV"""
    output_dir = "output"
    file_path = os.path.join(output_dir, "merged_data.csv")

    # ✅ Vérifier si le dossier existe, sinon le créer
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"📁 Dossier {output_dir} créé.")

    df.to_csv(file_path, index=False)
    logger.info(f"✅ Données sauvegardées dans {file_path}")


@flow(name="Prefect Data Pipeline")
def data_pipeline():
    """🚀 Orchestration du pipeline de transformation des données"""
    logger.info("🚀 Démarrage du pipeline Prefect...")
    
    # Étape 1 : Chargement des données
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren = load_data()
    
    # Étape 2 : Déballage de df_mina
    df_mina = unpack_minalogic(df_mina)
    
    # Étape 3 : Nettoyage des DataFrames
    df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren = clean_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren)
    
    # Étape 4 : Fusion des DataFrames
    merged_df = merge_data(df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_siren)
    
    # Étape 5 : Sauvegarde
    save_data(merged_df)

    logger.info("🏁 Pipeline terminé avec succès !")


# 🚀 Exécuter le flow
if __name__ == "__main__":
    data_pipeline()
