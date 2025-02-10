import os
import pandas as pd
import numpy as np
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
    df_ft = pd.read_csv(r'sources/frechtech full.csv')
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
    for df in [df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva, df_pepites, df_ft]:
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
    mots_cles_cols = ['mots_cles_z','mots_cles_x','mots_cles_y','mots_cles_a','product_types','sectors','mots_cles_b','mots_cles_t','mots_cles_ft','Marché','Field_1','Field_2','Field_3']
    merged_df["mots_cles_def"] = (
    merged_df[mots_cles_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    
    logger.info("Mots clés fusionnés")

    # Fusion des effectifs
    effectifs_cols = ['Tranche effectifs', 'employees']
    merged_df["Effectif_def"] = (
    merged_df[effectifs_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    logger.info("Effectifs fusionnés")

    # Fusion des sites internet
    site_web_cols = ['site_web', 'site_web_x','site_web_y','site_web_z','site_web_z','site_web_u','site_web_t']
    merged_df["site_web_def"] = (
    merged_df[site_web_cols]
    .stack()
    .groupby(level=0)
    .agg(lambda x: list(set(sum((y if isinstance(y, list) else [y] for y in x.dropna()), []))))
    )
    merged_df["site_web_def"] = merged_df["site_web_def"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    merged_df["site_web_def"] = merged_df["site_web_def"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
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
    #merged_df.drop(columns=financement_cols, inplace=True)
    merged_df.drop(columns=site_web_cols, inplace=True)
    merged_df.drop(columns=adresse_cols, inplace=True)
    merged_df.drop(columns=creation_cols, inplace=True)
    merged_df.drop(columns=effectifs_cols, inplace=True)
    merged_df.drop(columns=['emplacement','fundraising','Denomination légale','tags','dernier_financement'], inplace=True)
    merged_df = merged_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Supprimer les espaces en début/fin
    merged_df = merged_df.applymap(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)  # Remplacer les espaces multiples
    
    logger.info("Colonnes supprimées")

    # Fonction de nettoyage adaptée pour gérer à la fois les chaînes et les listes
    def clean_mots_cles(s):
        if isinstance(s, list):
            tokens = [str(token).strip() for token in s if str(token).strip() != ""]
            tokens = [str(token).strip() for token in s if str(token).strip() != "#"]

        elif isinstance(s, str):
            tokens = s.split(",")
            tokens = [token.strip() for token in tokens if token.strip() != ""]
            tokens = [token.strip() for token in tokens if token.strip() != '#']

        else:
            tokens = []
        return ", ".join(tokens)

    # Application de la fonction sur la colonne "mots_cles_def"
    merged_df['mots_cles_def'] = merged_df['mots_cles_def'].apply(clean_mots_cles)
        
    # Fonction pour extraire la date d'une cellule
    def extract_date(cell):
        # Si la cellule est une liste, on renvoie son premier élément
        if isinstance(cell, list):
            if len(cell) > 0:
                return cell[0]
            else:
                return None
        # Si la cellule est une chaîne qui ressemble à une liste, on tente de la convertir
        if isinstance(cell, str) and cell.startswith('[') and cell.endswith(']'):
            try:
                cell_list = ast.literal_eval(cell)
                if isinstance(cell_list, list) and len(cell_list) > 0:
                    return cell_list[0]
            except Exception as e:
                pass
        return cell
    
    logger.info("Mots clés nettoyés")


    # Netoyyage de 'date_creation_def'
    merged_df['date_creation_def'] = merged_df['date_creation_def'].apply(extract_date)
    # Conversion en datetime
    merged_df['date_creation_def'] = pd.to_datetime(merged_df['date_creation_def'], format='%Y-%m-%d', errors='coerce') # errors='coerce' convertit les valeurs incorrectes en NaT
    # On ne recupère que l'année
    #merged_df['date_creation_def'] = merged_df['date_creation_def'].dt.year

    # Netoyyage de 'Date dernier financement'
    merged_df['Date dernier financement'] = pd.to_datetime(merged_df['Date dernier financement'], format='%d.%m.%y')

    return merged_df

logger.info("Dates nettoyés")


@task
def to_missing(df):
    """Applique la transformation à toutes les cellules pour remplacer certaines valeurs par missing (np.nan)"""
    def missing_value(x):
        if x is None:
            return np.nan
        if isinstance(x, str):
            if x.strip().lower() in ["non disponible", "site web non disponible", "description non disponible", "none", "Catégorie non disponible"]:
                return np.nan
            return x
        if isinstance(x, list):
            cleaned = [str(item).strip().lower() for item in x if item is not None]
            if not cleaned or all(val in ["non disponible", "site web non disponible", "description non disponible", "none" ,"Catégorie non disponible"] for val in cleaned):
                return np.nan
            return x
        return x
    return df.applymap(missing_value)


@task
def clean_effectif(merged_df):
    """ Nettoie et standardise la colonne Effectif_def en conservant toujours la plus grande valeur. """

    # Définition du classement des tranches d'effectifs
    effectif_order = {
        "0 salarié": 0,
        "1 ou 2 salariés": 1,
        "3 à 5 salariés": 2,
        "6 à 9 salariés": 3,
        "10 à 19 salariés": 4,
        "20 à 49 salariés": 5,
        "50 à 99 salariés": 6,
        "100 à 199 salariés": 7,
        "200 à 499 salariés": 8,
        "500 à 999 salariés": 9,
        "1000 à 1999 salariés": 10,
        "2000 à 4999 salariés": 11,
        "5000 salariés ou plus": 12,
        "10 000 salariés et plus": 13,
        "Effectif inconnu": -1
    }

    # Traduction des effectifs de l'anglais vers le français
    effectif_translation = {
        '2-10 employees': "3 à 5 salariés",
        '11-50 employees': "20 à 49 salariés",
        '51-200 employees': "50 à 99 salariés",
        '201-500 employees': "200 à 499 salariés",
        '1 employees': "1 ou 2 salariés",
        '501-1000 employees': "500 à 999 salariés",
        '1001-5000 employees': "1000 à 1999 salariés",
        '10001+ employees': "5000 salariés ou plus",
        '5001-10000 employees': "5000 salariés ou plus",
        'NN': "Effectif inconnu"
    }

    def get_largest_effectif(effectif):
        """ Sélectionne la plus grande tranche d'effectif en convertissant les valeurs. """

        # 🔹 Gérer les valeurs NaN et vides
        if pd.isna(effectif) or effectif == [] or effectif == "":
            return np.nan
        
        # 🔹 Toujours transformer en liste pour un traitement uniforme
        if not isinstance(effectif, list):
            effectif = [effectif]

        # 🔹 Aplatir les listes imbriquées et convertir en texte
        flattened = []
        for item in effectif:
            if isinstance(item, list):
                flattened.extend(item)
            else:
                flattened.append(str(item).strip())

        cleaned_effectifs = []
        for e in flattened:
            # 🔹 Convertir les valeurs anglaises en français
            if e in effectif_translation:
                e = effectif_translation[e]

            # 🔹 Convertir les valeurs numériques seules (ex: "11" → "10 à 19 salariés")
            if e.isdigit():
                num = int(e)
                for label, rank in effectif_order.items():
                    if str(num) in label:
                        e = label
                        break
            
            # 🔹 Ajouter si reconnu
            if e in effectif_order:
                cleaned_effectifs.append(e)

        # 🔹 Retourne la plus grande tranche d'effectif
        return max(cleaned_effectifs, key=lambda x: effectif_order[x]) if cleaned_effectifs else np.nan

    # Correction : On force la sortie des listes en valeurs uniques avant d'appliquer la transformation
    merged_df["Effectif_def"] = merged_df["Effectif_def"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

    # Appliquer la transformation sur la colonne Effectif_def
    merged_df["Effectif_def"] = merged_df["Effectif_def"].apply(get_largest_effectif)

    logger.info("✅ Effectifs standardisés et nettoyés en conservant la plus grande tranche.")
    
    # 🔍 Ajout d'un log pour vérifier la distribution après nettoyage
    print("🔍 Vérification après clean_effectif :", merged_df["Effectif_def"].value_counts(dropna=False))

    return merged_df


@task
def split_contact(merged_df):
    def split_contact_row(contact):
        # Vérifie si contact est une chaîne valide
        if not isinstance(contact, str) or pd.isna(contact):
            # Vous pouvez retourner des valeurs par défaut (par exemple, des chaînes vides)
            return pd.Series({'Nom': '', 'Prenom': '', 'Poste': ''})
        
        # Sépare la chaîne en mots
        parts = contact.split()
        if len(parts) >= 2:
            nom = parts[0].upper()  # le nom en majuscules
            prenom = parts[1]
            # Tous les mots à partir du troisième vont dans 'Poste'
            poste = " ".join(parts[2:]) if len(parts) > 2 else ''
        else:
            # Si la chaîne ne comporte pas au moins deux mots
            nom = contact.upper()
            prenom = ''
            poste = ''
        return pd.Series({'Nom': nom, 'Prenom': prenom, 'Poste': poste})
    
    # Applique la fonction sur la colonne 'Contact'
    merged_df[['Nom', 'Prenom', 'Poste']] = merged_df['Contact'].apply(split_contact_row)
    merged_df.drop(columns=['Contact'], inplace=True)
    return merged_df


@task
def cleaning_funding(merged_df):
    def convert_montant(value):
        if isinstance(value, str):
            value = value.replace('€', '').replace(',', '.').strip()  # Nettoyage de base
            
            # Détection des unités et conversion manuelle
            if 'M' in value:
                return round(float(value.replace('M', '')) * 1_000_000)
            elif 'k' in value:
                return round(float(value.replace('k', '')) * 1_000)
            elif 'B' in value:
                return round(float(value.replace('B', '')) * 1_000_000_000)
            elif value.replace('.', '', 1).isdigit():  # Vérifie si c'est déjà un nombre valide
                return float(value)
            else:
                return np.nan  # Valeur invalide, renvoie NaN
        return np.nan  # Gère les valeurs non valides

    # Appliquer la conversion sur chaque valeur de la colonne
    merged_df['Montant_def'] = merged_df['Montant'].apply(convert_montant)
    
    # Conversion en float
    merged_df['Montant_def'] = merged_df['Montant_def'].astype(float)

    # Supprimer les colonnes inutiles
    merged_df.drop(columns=['financement', 'Montant'], inplace=True, errors='ignore')

    logger.info(f"Financement nettoyé et fusionné")

    return merged_df


#@task
#def fil_type_organisme(merged_df):


@task
def deduplicate_and_clean(merged_df):
    #Regroupe les lignes et fusionne les valeurs uniques sans supprimer de colonnes. """

    def merge_values(series):
        unique_values = pd.Series(series.dropna().unique())  # Supprime les NaN et garde valeurs uniques
        unique_values = unique_values.apply(str)  # Convertit en string pour éviter les problèmes
        return ", ".join(sorted(set(unique_values))) if not unique_values.empty else None  # Supprime doublons et trie
    
    # Vérifier si "Effectif_def" existe avant le groupby
    if "Effectif_def" not in merged_df.columns:
        logger.warning("⚠️ Effectif_def a disparu avant deduplicate_and_clean ! Elle sera recréée vide.")
        merged_df["Effectif_def"] = np.nan  # Crée une colonne vide si elle n'existe pas

    # Groupby sans perdre "Effectif_def"
    merged_df = merged_df.groupby("nom", as_index=False).agg(lambda x: merge_values(x) if x.name != "Effectif_def" else x.mode().max())

    print("✅ Vérification après deduplicate_and_clean:", merged_df.columns)  # Vérification
    print("🔍 Après deduplicate_and_clean :")
    print(merged_df["Effectif_def"].head(20))

    return merged_df


@task
def clean_keywords_task(merged_df):
    """ Nettoie la colonne mots_cles_def en supprimant les doublons et les caractères inutiles. """

    # Liste des caractères à supprimer
    remove_chars = ["[", "]", "'", '"', "#", "()", "{}"]

    def process_entry(entry):
        if pd.isna(entry) or entry in ["", "[]"]:  # Gérer les NaN et listes vides
            return None
        
        # 🔹 Convertir les tuples en listes
        if isinstance(entry, tuple):
            entry = list(entry)
        
        # 🔹 Convertir une chaîne qui ressemble à une liste en vraie liste
        if isinstance(entry, str) and entry.startswith("[") and entry.endswith("]"):
            try:
                entry = eval(entry)  # Convertir la chaîne en liste réelle
            except:
                return None  # Si erreur, ignorer

        # 🔹 Si ce n'est pas une liste, transformer en liste avec une seule valeur
        if not isinstance(entry, list):
            entry = [entry]
        
        # 🔹 Nettoyage : suppression des espaces et mise en minuscule
        clean_list = sorted(set(str(item).strip().lower() for item in entry if str(item).strip()))

        # 🔹 Suppression des caractères indésirables
        clean_list = [word.translate(str.maketrans("", "", "".join(remove_chars))) for word in clean_list]

        return ", ".join(clean_list) if clean_list else None  # Retourne une chaîne propre

    # Appliquer la fonction sur la colonne mots_cles_def
    merged_df["mots_cles_def"] = merged_df["mots_cles_def"].apply(process_entry)
    merged_df["Date dernier financement"] = pd.to_datetime(
        merged_df["Date dernier financement"], 
        format='%Y-%m-%d',  # ⚠️ Adapte ce format à ton jeu de données
        errors='coerce'  # 🔹 Convertit les erreurs en NaT (valeur manquante)
        )
    merged_df["date_creation_def"] = pd.to_datetime(
        merged_df["date_creation_def"], 
        format='%Y-%m-%d',  # ⚠️ Adapte ce format à ton jeu de données
        errors='coerce'  # 🔹 Convertit les erreurs en NaT (valeur manquante)
        )
    
    return merged_df


@task
def new_siren(merged_df):
    # Charger la table contenant les SIREN
    missing_siren_df = pd.read_csv("missing_siren_with_siren_complet.csv")
    # Normaliser les noms en majuscules
    missing_siren_df["nom"] = missing_siren_df["nom"].astype(str).str.upper()
    merged_df["nom"] = merged_df["nom"].astype(str).str.upper()
    # Fusionner sur 'nom' pour récupérer 'SIREN'
    merged_df = merged_df.merge(missing_siren_df[['nom', 'SIREN']], on='nom', how='left')
    # Fusionner les colonnes SIREN_x et SIREN_y
    if "SIREN_x" in merged_df.columns and "SIREN_y" in merged_df.columns:
        merged_df["SIREN"] = merged_df["SIREN_x"].combine_first(merged_df["SIREN_y"])
        merged_df.drop(columns=["SIREN_x", "SIREN_y"], inplace=True) 
    elif "SIREN_y" in merged_df.columns:
        merged_df.rename(columns={"SIREN_y": "SIREN"}, inplace=True) 

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
                       'Activité principale', 'Effectif_def', 'market',
                    'mots_cles_def', 'site_web_def', 'adresse_def',
                       'date_creation_def']
    for col in cols_to_convert:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].apply(make_hashable)
    
    # Table société avec créaton ID
    societes = merged_df[['nom', 'description', 'logo', "Type d'organisme", 'SIREN', 
                            'Activité principale', 'Effectif_def', 'market', 
                            'mots_cles_def', 'site_web_def', 'adresse_def', 
                            'date_creation_def']].drop_duplicates()
    societes.insert(0, "entreprise_id", range(1, len(societes) + 1))
    
    # Table des Personnes avec entreprise_id et création contact_id
    personnes = merged_df[['nom', 'Nom', 'Prenom', 'Poste']].drop_duplicates()
    personnes = personnes.merge(societes[['nom', 'entreprise_id']], on='nom', how='left')
    personnes = personnes[['entreprise_id', 'Nom', 'Prenom', 'Poste']]
    personnes.insert(0, "contact_id", range(1, len(personnes) + 1))
    
    # Table des Financements avec entreprise_id et création de financement_id
    financements = merged_df[['nom', 'Date dernier financement', 'Série', 'Montant_def',
                                'valeur_entreprise']].drop_duplicates()
    financements = merged_df[['nom', 'Date dernier financement', 'Série', 'Montant_def',
                                'valeur_entreprise']].drop_duplicates()
    financements = financements.merge(societes[['nom', 'entreprise_id']], on='nom', how='left')
    financements = financements[['entreprise_id', 'Date dernier financement', 'Série', 'Montant_def',
                                   'valeur_entreprise']]
    financements = financements[['entreprise_id', 'Date dernier financement', 'Série', 'Montant_def',
                                   'valeur_entreprise']]
    financements.insert(0, "financement_id", range(1, len(financements) + 1))
    
    # Sauvegarde des datasets
    societes.to_csv("societes.csv", index=False)
    personnes.to_csv("personnes.csv", index=False)
    financements.to_csv("financements.csv", index=False)
    
    logger.info("Les trois datasets ont été créés.")


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
    # Suppression des valeurs nul / etc. 
    merged_df = to_missing(merged_df)
    # Création des colonnes contacts 
    merged_df = split_contact(merged_df)
    # Cleaning de la partie financement / montants 
    merged_df = cleaning_funding(merged_df)
    # Dédoublonnage !!!!! 
    merged_df = deduplicate_and_clean(merged_df)
    # Standardisation des effectifs 
    merged_df["Effectif_def"] = merged_df["Effectif_def"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    merged_df  = clean_effectif(merged_df)
    #Clean mot clés
    merged_df = clean_keywords_task(merged_df)
    #Ajout des SIREN de l'API SIREN
    merged_df = new_siren(merged_df) 
    # Sauvegarde
    save_data(merged_df)
    # Création de la multibase de données
    create_database(merged_df)
    


if __name__ == "__main__":
    data_pipeline()
