if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def cleaning(df_mina, df_CESFR, df_viva, df_bpi, df_tech, df_maddy, df_siren):
    """
    Transforme les noms de colonnes et retourne les DataFrames sous forme de dictionnaire.
    """
    print("🔍 Vérification des entrées dans cleaning:")
    for name, df in zip(["df_viva", "df_bpi", "df_maddy", "df_mina", "df_CESFR", "df_siren", "df_tech"], 
                         [df_viva, df_bpi, df_maddy, df_mina, df_CESFR, df_siren, df_tech]):
        print(f"{name}: {type(df)}, shape: {df.shape}")

    # 📌 Vérifier les colonnes avant suppression
    print(f"📊 Colonnes disponibles dans df_tech avant suppression: {df_tech.columns.tolist()}")

    # ✅ Suppression sécurisée des colonnes 
    def safe_drop(df, cols):
        existing_cols = [col for col in cols if col in df.columns]
        df.drop(columns=existing_cols, inplace=True, errors='ignore')

    # Appliquer la suppression sécurisée
    safe_drop(df_tech, ['social_links'])
    safe_drop(df_bpi, ['total_funding'])
    safe_drop(df_CESFR, ['Lien'])
    safe_drop(df_maddy, ['Siège', 'Date de création'])
    safe_drop(df_mina, ['url', "Date d'adhésion", "contact", "infos"])
    safe_drop(df_viva, ["link", "looking_for", "development_level"])
    safe_drop(df_siren, ["État administratif", "Catégorie entreprise", "Denomination usuelle"])

    print("✅ Suppression des colonnes terminée.")

    # 🔄 Renommage des colonnes
    rename_mappings = {
        df_bpi: {'name': 'nom', 'hashtags': 'mots_cles_b', 'website': 'site_web'},
        df_CESFR: {'Nom': 'nom', 'Description': 'description', 'Catégories': 'mots_cles_y', 
                   'Website': 'site_web_z', 'Adresse': 'adresse_z', 'Logo': 'logo'},
        df_maddy: {'Nom': 'nom', 'Description': 'description', 'Site internet': 'site_web', 
                   'Logo': 'logo', 'Hashtags': 'mots_cles_z'},
        df_mina: {'name': 'nom', 'Logo': 'logo'},
        df_viva: {'name': 'nom', 'website': 'site_web_u', 'Logo': 'logo'}
    }

    for df, rename_map in rename_mappings.items():
        df.rename(columns=rename_map, inplace=True)

    print("✅ Renommage des colonnes terminé.")

    # 🔠 Convertir les noms en majuscules
    for df in [df_bpi, df_tech, df_maddy, df_CESFR, df_mina, df_viva]:
        if 'nom' in df.columns:
            df['nom'] = df['nom'].str.upper()

    print("✅ Conversion des noms en majuscules terminée.")

    # 🔄 Fusion des DataFrames
    merged_df = df_bpi
    for df in [df_tech, df_maddy, df_CESFR, df_mina, df_viva]:
        merged_df = pd.merge(merged_df, df, on=['nom', 'description', 'logo'], how='outer')

    merged_df = pd.merge(merged_df, df_siren, on=['nom'], how='outer')

    print("✅ Fusion des DataFrames terminée.")

    # 📌 Remplissage des valeurs manquantes
    site_web_cols = ['site_web_x', 'site_web_y', 'site_web_z', 'site_web', 'site_web_u']
    for col in site_web_cols[1:]:
        merged_df[site_web_cols[0]].fillna(merged_df[col], inplace=True)

    mots_cles_cols = ['mots_cles_b', 'mots_cles', 'mots_cles_z', 'mots_cles_y', 'mots_cles_a']
    for col in mots_cles_cols[1:]:
        merged_df[mots_cles_cols[0]].fillna(merged_df[col], inplace=True)

    merged_df['mots_cles_u'] = merged_df[['Field_1', 'Field_2', 'Field_3']].fillna('').agg(' '.join, axis=1)
    merged_df['mots_cles_b'].fillna(merged_df['mots_cles_u'], inplace=True)

    merged_df['adresse_z'].fillna(merged_df[['city_x', 'city_y', 'Adresse']].bfill(axis=1).iloc[:, 0], inplace=True)
    merged_df['Date de création_x'].fillna(merged_df['Date de création_y'], inplace=True)
    merged_df['Montant'].fillna(merged_df['fundraising'], inplace=True)

    print("✅ Remplissage des valeurs terminée.")

    # 🚀 Suppression des colonnes inutiles
    drop_cols = site_web_cols[1:] + mots_cles_cols[1:] + ['mots_cles_u', 'Field_1', 'Field_2', 'Field_3', 
                 'city_x', 'city_y', 'Adresse', 'Date de création_y', 'emplacement', 'fundraising']
    safe_drop(merged_df, drop_cols)

    print("✅ Suppression des colonnes inutiles terminée.")

    # 📌 Fonctions d'agrégation pour la fusion finale
    def combine_strings(series):
        """Retourne la première chaîne non vide/nulle."""
        for val in series:
            if isinstance(val, str) and val.strip() != "":
                return val
        return None

    def combine_lists(series):
        """Fusionne toutes les listes en un set (pour éviter les doublons), puis retransforme en liste."""
        combined = set()
        for val in series.dropna():
            if isinstance(val, list):
                combined.update([x for x in val if x])  # On évite les chaînes vides
        return list(combined) if combined else None

    def combine_first_nonnull(series):
        """Renvoie la première valeur non-nulle, quel que soit son type."""
        for val in series:
            if pd.notnull(val):
                return val
        return None

    aggregation_dict = {
        'nom': 'first',
        'description': combine_strings,
        'logo': combine_strings,
        'mots_cles_b': combine_lists,
        'site_web_x': combine_strings,
        'tags': combine_lists,
        'Date dernier financement': combine_strings,
        'Série': combine_strings,
        'Montant': combine_strings,
        'LinkedIn': combine_strings,
        'adresse_z': combine_strings,
        'Contact': combine_strings,
        'Date de création_x': combine_strings,
        'Marché': combine_strings,
        "Type d'organisme": combine_strings,
        'SIREN': combine_strings,
        'Activité principale': combine_strings,
        'Tranche effectifs': combine_strings
    }

    merged_df = merged_df.groupby('nom', as_index=False).agg(aggregation_dict)

    print("✅ Agrégation terminée. Nettoyage terminé.")

    return merged_df
