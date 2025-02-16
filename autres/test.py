import pandas as pd
import requests
import time

# Charger votre fichier CSV
df = pd.read_csv("api_siren_final.csv")

# Fonction pour appeler l'API SIREN et récupérer les infos supplémentaires
def get_siren_info(siren):
    url = f"https://api.insee.fr/api-sirene/3.11/siret/{siren}"  # L'URL inclut le SIREN
    api_key = "0d23c27b-5bdb-4fd9-a3c2-7b5bdb6fd934"  # Remplacer par votre clé API
    
    # Définir les headers avec la clé API
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    # Effectuer la requête GET
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        
        # Extraire les informations pertinentes
        adresse = data.get('adresse', {}).get('libelle', '')
        ville = data.get('adresse', {}).get('commune', '')
        date_creation = data.get('date_creation', '')
        fermeture = data.get('date_fermeture', 'Non fermée')
        effectif = data.get('effectif', '')
        code_naf = data.get('code_naf', '')
        categorie_entreprise = data.get('categorie_entreprise', '')
        dirigeant_contact = data.get('contact', {}).get('nom', '') + ' ' + data.get('contact', {}).get('prenom', '')
        
        # Ajouter les coordonnées Lambert
        coord_abscisse = data.get('coordonneeLambertAbscisseEtablissement', '')
        coord_ordonnee = data.get('coordonneeLambertOrdonneeEtablissement', '')
        
        return {
            "adresse": adresse,
            "ville": ville,
            "date_creation": date_creation,
            "fermeture": fermeture,
            "effectif": effectif,
            "code_naf": code_naf,
            "categorie_entreprise": categorie_entreprise,
            "dirigeant_contact": dirigeant_contact,
            "coord_abscisse": coord_abscisse,
            "coord_ordonnee": coord_ordonnee
        }
    else:
        print(f"Erreur lors de l'appel de l'API pour le SIREN {siren}: {response.status_code}")
        return {}

# Ajouter les colonnes au DataFrame en faisant des appels API
for index, row in df.iterrows():
    siren = row['SIREN']
    siren_info = get_siren_info(siren)
    
    for key, value in siren_info.items():
        df.at[index, key] = value

    # Ajouter une pause de 1 seconde entre les requêtes pour ne pas dépasser la limite
    time.sleep(1)

# Sauvegarder le DataFrame mis à jour dans un nouveau fichier CSV
df.to_csv("api_siren_final_updated.csv", index=False)

print("Mise à jour terminée et sauvegardée dans 'api_siren_final_updated.csv'")
