import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import json

# Configuration de Selenium
driver = webdriver.Chrome()  # ou webdriver.Firefox(), etc.
driver.get("https://www.ftalps.com/directory/annuaire-startups/")

# Attendre que la page se charge
time.sleep(3)

# Cliquer sur "Afficher plus" jusqu'à ce qu'il disparaisse
print("Début du chargement des startups...")
while True:
    try:
        load_more_button = driver.find_element(By.ID, "loadMoreCompanies")
        load_more_button.click()
        print("Bouton 'Afficher plus' cliqué.")
        time.sleep(2)  # Attendre que les nouveaux éléments se chargent
    except Exception as e:
        print("Plus de bouton 'Afficher plus' ou erreur :", e)
        break

# Attendre que les éléments soient chargés
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "CompanyCard-link")))

# Récupérer le contenu de la page après avoir tout chargé
page_source = driver.page_source
driver.quit()

# Utiliser BeautifulSoup pour parser le contenu
soup = BeautifulSoup(page_source, "html.parser")

# Récupérer tous les liens des startups
startup_links = []
for link in soup.find_all("a", class_="CompanyCard-link"):  # Ajustez le sélecteur si nécessaire
    full_link = "https://www.ftalps.com" + link["href"]
    startup_links.append(full_link)
    print("Lien trouvé :", full_link)

print(f"{len(startup_links)} liens de startups récupérés.")

# Fonction pour extraire les détails d'une startup
def get_startup_details(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Vérifier que les éléments sont bien trouvés
        name = soup.find("h1", class_="Company-title")
        if not name:
            print(f"Aucun nom trouvé pour {url}")
            return None

        details = {
            "name": name.text.strip(),
            "logo": soup.find("img", class_="Company-image")["src"] if soup.find("img", class_="Company-image") else None,
            "description_simple": soup.find("div", class_="Company-descSimple").text.strip() if soup.find("div", class_="Company-descSimple") else None,
            "description_detail": soup.find("div", class_="Company-descDetail").text.strip() if soup.find("div", class_="Company-descDetail") else None,
            "last_update": soup.find("div", class_="Company-update").text.strip() if soup.find("div", class_="Company-update") else None,
            "team": [],
            "address": soup.find("div", class_="Company-adress").text.strip() if soup.find("div", class_="Company-adress") else None,
            "phone": soup.find("a", class_="Company-tel")["href"].replace("tel:", "") if soup.find("a", class_="Company-tel") else None,
            "email": soup.find("a", class_="Company-mail")["href"].replace("mailto:", "") if soup.find("a", class_="Company-mail") else None,
            "website": soup.find("a", class_="Company-site")["href"] if soup.find("a", class_="Company-site") else None,
            "sectors": [tag.text.strip() for tag in soup.find_all("li", class_="Company-sideTag")],
        }

        # Récupérer les membres de l'équipe
        for team_member in soup.find_all("div", class_="Company-contact"):
            member = {
                "name": team_member.find("div", class_="Company-contactName").text.strip() if team_member.find("div", class_="Company-contactName") else None,
                "job": team_member.find("div", class_="Company-contactJob").text.strip() if team_member.find("div", class_="Company-contactJob") else None,
            }
            details["team"].append(member)

        return details
    except Exception as e:
        print(f"Erreur lors de l'extraction des détails pour {url}: {e}")
        return None

# Récupérer les détails de toutes les startups
all_startups = []
for link in startup_links:
    try:
        print(f"Visite de la page : {link}")
        startup_details = get_startup_details(link)
        if startup_details:  # Ne sauvegarder que si les détails sont valides
            all_startups.append(startup_details)
            print(f"Scraped: {startup_details['name']}")
    except Exception as e:
        print(f"Error scraping {link}: {e}")

# Sauvegarder les résultats dans un fichier JSON
with open("startups.json", "w", encoding="utf-8") as f:
    json.dump(all_startups, f, ensure_ascii=False, indent=4)

print(f"{len(all_startups)} startups scrapées et sauvegardées dans startups.json.")