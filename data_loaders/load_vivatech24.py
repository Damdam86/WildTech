from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_from_s3_bucket(**kwargs) -> DataFrame:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'wildstartech'  # Change to your bucket name
    object_key = 'partners_viva_tech.csv'   # Change to your object key

    df_viva = S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
    bucket_name,
    object_key,
    format='csv',
    delimiter=',',  
    quotechar='"',  # Gère les textes contenant des virgules
    encoding='utf-8',  # Assure la compatibilité avec les caractères spéciaux
    skip_blank_lines=True,  # Ignore les lignes vides
    on_bad_lines='skip'  # Ignore les erreurs de parsing au lieu de planter
)
    return df_viva
#https://docs.mage.ai/integrations/databases/S3