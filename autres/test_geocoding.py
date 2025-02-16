import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

# 🔹 Charger le fichier contenant les adresses
file_path = "data_adresse.csv"
df = pd.read_csv(file_path)

# 🔹 Initialiser le geocodeur avec Nominatim
geocoder = Nominatim(user_agent="geo_app")

# 🔹 Fonction de Geocoding avec gestion des erreurs et des timeouts
def get_coordinates(address, max_retries=3):
    if pd.isna(address) or address.strip() == "":
        return None, None  # Retourne NaN si l'adresse est vide

    retries = 0
    while retries < max_retries:
        try:
            location = geocoder.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None  # Aucun résultat trouvé
        except GeocoderTimedOut:
            retries += 1
            print(f"⏳ Timeout sur '{address}', tentative {retries}/{max_retries}...")
            time.sleep(2)  # Attente avant de réessayer
    
    print(f"❌ Impossible de géocoder : {address}")
    return None, None  # Retourne NaN si le geocoding échoue

# 🔹 Appliquer le Geocoding avec affichage de la progression
batch_size = 10  # Sauvegarde toutes les 10 sociétés
output_path = "data_adresse_geocoded.csv"

# Charger un fichier existant pour reprendre si besoin
try:
    df_existing = pd.read_csv(output_path)
    already_geocoded = df_existing.dropna(subset=['latitude', 'longitude'])
    df = df[~df['adresse_def'].isin(already_geocoded['adresse_def'])]  # Filtrer les adresses non traitées
    print(f"✅ Reprise du fichier existant ({len(already_geocoded)} adresses déjà traitées).")
except FileNotFoundError:
    already_geocoded = pd.DataFrame()

# Liste pour stocker les nouvelles coordonnées
geocoded_data = []

for i, row in df.iterrows():
    lat, lon = get_coordinates(row['adresse_def'])
    geocoded_data.append([row['adresse_def'], lat, lon])

    # Affichage de la progression
    print(f"📍 {i+1}/{len(df)} → {row['adresse_def']} → ({lat}, {lon})")

    # Sauvegarde toutes les 10 lignes
    if (i + 1) % batch_size == 0 or (i + 1) == len(df):
        temp_df = pd.DataFrame(geocoded_data, columns=['adresse_def', 'latitude', 'longitude'])
        final_df = pd.concat([already_geocoded, temp_df]).drop_duplicates(subset=['adresse_def'])
        final_df.to_csv(output_path, index=False)
        print(f"💾 Sauvegarde automatique à {i+1} adresses.")

print("✅ Géocodage terminé ! Données enregistrées dans", output_path)
