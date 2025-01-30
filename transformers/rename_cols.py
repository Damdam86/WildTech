if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def rename_cols(df_bpi, df_CESFR, df_maddy, df_mina, df_viva):
    """
    Transforme les noms de colonnes et retourne les DataFrames sous forme de dictionnaire.
    """
    df_bpi = df_bpi.rename(columns={'name': 'nom', 'hashtags': 'mots_cles_b', 'website': 'site_web'})
    df_CESFR = df_CESFR.rename(columns={'Nom': 'nom', 'Description': 'description', 
                                        'Catégories': 'mots_cles_y', 'Website': 'site_web_z', 
                                        'Adresse': 'adresse_z', 'Logo': 'logo'})
    df_maddy = df_maddy.rename(columns={'Nom': 'nom', 'Description': 'description', 
                                        'Site internet': 'site_web', 'Logo': 'logo', 'Hashtags': 'mots_cles_z'})
    df_mina = df_mina.rename(columns={'name': 'nom', 'Logo': 'logo'})
    df_viva = df_viva.rename(columns={'name': 'nom', 'website': 'site_web_u', 'Logo': 'logo'})

    print("✅ Colonnes finales de df_bpi AVANT retour :", df_bpi.columns)

    return {
        "df_bpi": df_bpi,
        "df_CESFR": df_CESFR,
        "df_maddy": df_maddy,
        "df_mina": df_mina,
        "df_viva": df_viva
    }

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
