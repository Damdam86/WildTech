if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(drop_cols_output, *args, **kwargs):
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

    df_bpi = drop_cols_output["df_bpi"]
    df_CESFR = drop_cols_output["df_CESFR"]
    df_maddy = drop_cols_output["df_maddy"]
    df_mina = drop_cols_output["df_mina"]
    df_viva = drop_cols_output["df_viva"]
    df_siren = drop_cols_output["df_siren"]
    df_tech = drop_cols_output["df_tech"]

    # Convertir les colonnes 'nom' en majuscules
    df_bpi['nom'] = df_bpi['nom'].str.upper()
    df_tech['nom'] = df_tech['nom'].str.upper()
    df_maddy['nom'] = df_maddy['nom'].str.upper()
    df_CESFR['nom'] = df_CESFR['nom'].str.upper()
    df_mina['nom'] = df_mina['nom'].str.upper()
    df_viva['nom'] = df_viva['nom'].str.upper()

    return {
            "df_bpi": df_bpi,
            "df_CESFR": df_CESFR,
            "df_maddy": df_maddy,
            "df_mina": df_mina,
            "df_viva": df_viva,
            "df_siren" : df_siren,
            "df_tech" : df_tech
        }

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
