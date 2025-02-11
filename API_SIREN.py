import requests
import pandas as pd
import time

# 📌 Charger les données contenant les SIREN
file_path = "api_siren_final.csv"  # Remplace avec le bon fichier CSV
df = pd.read_csv(file_path)

# 🔄 Ajouter des colonnes pour stocker les résultats
columns_to_add = [
    "SIRET", "Numéro de voie", "Type de voie", "Nom de la voie", "Code postal", "Commune", "Adresse complète",
    "Date de création", "Date de fermeture", "Effectif salarié", "Code NAF", "Catégorie entreprise",
    "Dirigeant principal", "Coordonnée Lambert X", "Coordonnée Lambert Y"
]

for col in columns_to_add:
    if col not in df.columns:
        df[col] = ""

# 📌 Fonction pour récupérer les informations de l'entreprise via l'API
def get_company_info(siren):
    api_url = f"https://recherche-entreprises.api.gouv.fr/search?q={siren}"
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        if data.get("results"):
            company = data["results"][0]  # On prend le premier résultat
            siege = company.get("siege", {})
            dirigeants = company.get("dirigeants", [])

            # On récupère le premier dirigeant s'il existe
            dirigeant_principal = " / ".join([
                f"{dirigeant.get('nom', '')} {dirigeant.get('prenoms', '')}".strip()
                for dirigeant in dirigeants
            ]) if dirigeants else "Non disponible"

            return {
                "SIRET": siege.get("siret", "Non disponible"),
                "Numéro de voie": siege.get("numero_voie", "Non disponible"),
                "Type de voie": siege.get("type_voie", "Non disponible"),
                "Nom de la voie": siege.get("libelle_voie", "Non disponible"),
                "Code postal": siege.get("code_postal", "Non disponible"),
                "Commune": siege.get("libelle_commune", "Non disponible"),
                "Adresse complète": siege.get("geo_adresse", "Non disponible"),
                "Date de création": company.get("date_creation", "Non disponible"),
                "Date de fermeture": company.get("date_fermeture", "Non fermée"),
                "Effectif salarié": company.get("tranche_effectif_salarie", "Non disponible"),
                "Code NAF": company.get("activite_principale", "Non disponible"),
                "Catégorie entreprise": company.get("categorie_entreprise", "Non disponible"),
                "Dirigeant principal": dirigeant_principal,
                "Coordonnée Lambert X": siege.get("latitude", "Non disponible"),
                "Coordonnée Lambert Y": siege.get("longitude", "Non disponible"),
            }
    else:
        print(f"❌ Erreur API {response.status_code} pour SIREN {siren}: {response.text}")

    return {}

# 🔄 Boucle sur les entreprises
for index, row in df.iterrows():
    siren = str(row["SIREN"])
    print(f"🔍 Recherche des informations pour {siren} ({index+1}/{len(df)})")

    company_info = get_company_info(siren)

    if company_info:
        for key, value in company_info.items():
            df.at[index, key] = value

    # 📌 Sauvegarde partielle toutes les 10 entreprises
    if (index + 1) % 10 == 0:
        df.to_csv(f"partial_data_{index + 1}.csv", index=False)
        print(f"💾 Sauvegarde partielle : partial_data_{index + 1}.csv")

# 📌 Sauvegarde finale
output_file = "entreprises_completes.csv"
df.to_csv(output_file, index=False)
print(f"✅ Fichier final créé : {output_file}")
