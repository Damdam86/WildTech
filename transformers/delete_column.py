if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df_tech, df_bpi, df_CESFR, df_maddy, df_mina, df_viva, df_siren):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Suppression colonne
    df_tech.drop(columns=['social_links'], inplace=True)
    df_bpi.drop(columns=['total_funding'], inplace=True)

    df_CESFR.drop(columns=['Lien'], inplace=True)

    df_maddy.drop(columns=['Siège'], inplace=True)
    df_maddy.drop(columns=['Date de création'], inplace=True)

    df_mina.drop(columns=['url'], inplace=True)
    df_mina.drop(columns=["Date d'adhésion"], inplace=True)
    df_mina.drop(columns=["contact"], inplace=True)
    df_mina.drop(columns=["infos"], inplace=True)

    df_viva.drop(columns=["link"], inplace=True)
    df_viva.drop(columns=["looking_for"], inplace=True)
    df_viva.drop(columns=["development_level"], inplace=True)

    df_siren.drop(columns=["État administratif"], inplace=True)
    df_siren.drop(columns=["Catégorie entreprise"], inplace=True)
    df_siren.drop(columns=["Denomination usuelle"], inplace=True)

    return df_tech, df_bpi, df_CESFR, df_maddy, df_mina, df_viva, df_siren


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
