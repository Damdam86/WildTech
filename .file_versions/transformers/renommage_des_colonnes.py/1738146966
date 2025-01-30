if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df_bpi, df_CESFR, df_maddy, df_mina, df_viva):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    #Renomage de la colonne name en nom
    df_bpi = df_bpi.rename(columns={'name': 'nom'})
    df_bpi = df_bpi.rename(columns={'hashtags': 'mots_cles_b'})
    df_bpi = df_bpi.rename(columns={'website': 'site_web'})

    df_CESFR = df_CESFR.rename(columns={'Nom': 'nom'})
    df_CESFR = df_CESFR.rename(columns={'Description': 'description'})
    df_CESFR = df_CESFR.rename(columns={'Catégories': 'mots_cles_y'})
    df_CESFR = df_CESFR.rename(columns={'Website': 'site_web_z'})
    df_CESFR = df_CESFR.rename(columns={'Adresse': 'adresse_z'})
    df_CESFR = df_CESFR.rename(columns={'Logo': 'logo'})


    df_maddy = df_maddy.rename(columns={'Nom': 'nom'})
    df_maddy = df_maddy.rename(columns={'Description': 'description'})
    df_maddy = df_maddy.rename(columns={'Site internet': 'site_web'})
    df_maddy = df_maddy.rename(columns={'Logo': 'logo'})
    df_maddy = df_maddy.rename(columns={'Hashtags': 'mots_cles_z'})

    df_mina = df_mina.rename(columns={'name': 'nom'})
    df_mina = df_mina.rename(columns={'Logo': 'logo'})

    df_viva = df_viva.rename(columns={'name': 'nom'})
    df_viva = df_viva.rename(columns={'website': 'site_web_u'})
    df_viva = df_viva.rename(columns={'Logo': 'logo'})

    return df_bpi, df_CESFR, df_maddy, df_mina, df_viva


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
