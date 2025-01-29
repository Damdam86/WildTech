import requests
import pandas as pd
import time
from urllib.parse import quote

file_path = "company_names_list.csv"
df = pd.read_csv(file_path)

# Ajouter des colonnes pour stocker les résultats si elles n'existent pas
for col in [
    "SIREN", "Date de création", "Denomination légale", "Denomination usuelle",
    "Activité principale", "Tranche effectifs", "Catégorie entreprise", "État administratif"
]:
    if col not in df.columns:
        df[col] = ""

# 🔑 Votre clé API
api_key = "0d23c27b-5bdb-4fd9-a3c2-7b5bdb6fd934"

# URL de l'API SIRENE
api_url = "https://api.insee.fr/api-sirene/3.11/siret"

# Initialisation du compteur de requêtes
request_count = 0
request_limit = 30  # Limite d'appels par minute imposée par l'API

# Fonction pour interroger l'API
def fetch_company_data(company_name):
    global request_count
    
    # Encoder le nom de l'entreprise
    encoded_name = quote(company_name)
    
    # Paramètres de la requête
    params = {
        "q": f"denominationUniteLegale:{encoded_name}",
        "champs": (
            "siren,dateCreationUniteLegale,denominationUniteLegale,"
            "denominationUsuelle1UniteLegale,activitePrincipaleUniteLegale,"
            "trancheEffectifsUniteLegale,categorieEntreprise"
        ),
        "nombre": 1,
        "masquerValeursNulles": "true"
    }
    
    # En-têtes pour l'authentification
    headers = {
        "Accept": "application/json",
        "X-INSEE-Api-Key-Integration": api_key
    }
    
    # Vérification du quota
    if request_count >= request_limit:
        print("⏸️ Limite atteinte, pause de 60 secondes...")
        time.sleep(60)  # Pause de 60 secondes
        request_count = 0  # Réinitialiser le compteur

    # Exécuter la requête
    response = requests.get(api_url, headers=headers, params=params)
    request_count += 1  # Incrémenter le compteur

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Résultat API pour {company_name} : {data}")
        if "etablissements" in data and len(data["etablissements"]) > 0:
            etablissement = data["etablissements"][0]
            unite_legale = etablissement.get("uniteLegale", {})
            
            # Extraire les champs nécessaires
            siren = etablissement.get("siren", "Non disponible")
            date_creation = unite_legale.get("dateCreationUniteLegale", "Non disponible")
            denomination_legale = unite_legale.get("denominationUniteLegale", "Non disponible")
            denomination_usuelle = unite_legale.get("denominationUsuelle1UniteLegale", "Non disponible")
            activite_principale = unite_legale.get("activitePrincipaleUniteLegale", "Non disponible")
            tranche_effectifs = unite_legale.get("trancheEffectifsUniteLegale", "Non disponible")
            categorie_entreprise = unite_legale.get("categorieEntreprise", "Non disponible")
            etat_administratif = unite_legale.get("etatAdministratifUniteLegale", "Non disponible")
            
            # Log des champs manquants
            missing_fields = [
                field for field, value in {
                    "siren": siren, "dateCreationUniteLegale": date_creation,
                    "denominationUniteLegale": denomination_legale,
                    "activitePrincipaleUniteLegale": activite_principale,
                    "trancheEffectifsUniteLegale": tranche_effectifs,
                    "categorieEntreprise": categorie_entreprise
                }.items() if value == "Non disponible"
            ]
            if missing_fields:
                print(f"⚠️ Champs manquants pour {company_name} : {', '.join(missing_fields)}")
            
            return (
                siren, date_creation, denomination_legale, denomination_usuelle,
                activite_principale, tranche_effectifs, categorie_entreprise, etat_administratif
            )
    else:
        print(f"❌ Erreur API pour {company_name} : {response.status_code} - {response.text}")
    return (None,) * 8  # Renvoie None pour signaler une absence de données

# Boucle sur les entreprises
for index, row in df.iterrows():
    company_name = row["nom"]
    print(f"🔍 Recherche pour : {company_name} ({index + 1}/{len(df)})")
    
    # Appel API pour récupérer les données
    (
        siren, date_creation, denomination_legale, denomination_usuelle,
        activite_principale, tranche_effectifs, categorie_entreprise, etat_administratif
    ) = fetch_company_data(company_name)
    
    # Mise à jour du DataFrame
    df.at[index, "SIREN"] = siren if siren else "Non disponible"
    df.at[index, "Date de création"] = date_creation if date_creation else "Non disponible"
    df.at[index, "Denomination légale"] = denomination_legale if denomination_legale else "Non disponible"
    df.at[index, "Denomination usuelle"] = denomination_usuelle if denomination_usuelle else "Non disponible"
    df.at[index, "Activité principale"] = activite_principale if activite_principale else "Non disponible"
    df.at[index, "Tranche effectifs"] = tranche_effectifs if tranche_effectifs else "Non disponible"
    df.at[index, "Catégorie entreprise"] = categorie_entreprise if categorie_entreprise else "Non disponible"
    df.at[index, "État administratif"] = etat_administratif if etat_administratif else "Non disponible"

    # Sauvegarde partielle toutes les 10 entreprises
    if (index + 1) % 10 == 0:
        partial_save_path = f"partial_data_{index + 1}.csv"
        df.to_csv(partial_save_path, index=False)
        print(f"💾 Sauvegarde partielle : {partial_save_path}")
    
    # Pause pour respecter les quotas
    time.sleep(2)

# Sauvegarde finale
output_file = "enriched_company_data.csv"
df.to_csv(output_file, index=False)
print(f"✅ Fichier final créé : {output_file}")
