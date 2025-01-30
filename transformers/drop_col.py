if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def drop_cols(data, df_siren, df_tech):
    """
    Supprime des colonnes spécifiques des DataFrames.
    """
    print('Hello')
    df_bpi = data.get("df_bpi")
    df_CESFR = data.get("df_CESFR")
    df_maddy = data.get("df_maddy")
    df_mina = data.get("df_mina")
    df_viva = data.get("df_viva")

    df_tech.drop(columns=['social_links'], inplace=True)
    df_bpi.drop(columns=['total_funding'], inplace=True)
    df_CESFR.drop(columns=['Lien'], inplace=True)
    df_maddy.drop(columns=['Siège', 'Date de création'], inplace=True)
    df_mina.drop(columns=['url', "Date d'adhésion", "contact", "infos"], inplace=True)
    df_viva.drop(columns=["link", "looking_for", "development_level"], inplace=True)
    df_siren.drop(columns=["État administratif", "Catégorie entreprise", "Denomination usuelle"], inplace=True)

    return {
            "df_bpi": df_bpi,
            "df_CESFR": df_CESFR,
            "df_maddy": df_maddy,
            "df_mina": df_mina,
            "df_viva": df_viva,
            "df_siren" : df_siren,
            "df_tech" : df_tech
        }