import pandas as pd
import logging
import ast

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)  # Logger pour afficher les logs Mage.ai

@transformer
def debal_mina(df_mina):
    # Débalage des colonnes
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

    return df_mina