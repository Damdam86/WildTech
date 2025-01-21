from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time

# Chemin vers le ChromeDriver
chrome_driver_path = r"C:\\chromedriver\\chromedriver.exe"  # Remplacez par le chemin correct

# Configuration des options Chrome
options = Options()
options.add_argument("--start-maximized")
options.add_argument("--disable-notifications")
options.add_argument("--headless")  # Optionnel : pour exécuter sans ouvrir le navigateur

# Initialisation du driver
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=options)

# URL de la page cible
url = "https://minalogicbusinessmeetings-2024.vimeet.events"  # Remplacez par l'URL cible

try:
    # Ouvrir la page
    driver.get(url)
    time.sleep(5)  # Attendre le chargement de la page

    # Scroll vers le bas pour charger toutes les entreprises
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Attendre le chargement des nouveaux contenus
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Récupérer toutes les cartes d'entreprises
    company_cards = driver.find_elements(By.CLASS_NAME, "catalog__item")
    print(f"Nombre d'entreprises trouvées : {len(company_cards)}")

    # Liste pour stocker les informations
    company_data = []

    # Extraction des informations de chaque carte
    for card in company_cards:
        try:
            # Nom de l'entreprise
            name_element = card.find_element(By.CLASS_NAME, "previewRecto-container")
            name = name_element.text.split("\n")[0]

            # Description
            description_element = card.find_element(By.CLASS_NAME, "text-truncate")
            description = description_element.text

            # Lien du site web
            link_element = card.find_element(By.TAG_NAME, "a")
            website = link_element.get_attribute("href")

            # Ajouter les informations à la liste
            company_data.append({
                "name": name,
                "description": description,
                "website": website
            })
        except Exception as e:
            print(f"Erreur lors de l'extraction d'une carte : {e}")

    # Afficher les données extraites
    for company in company_data:
        print(company)

finally:
    # Fermer le driver
    driver.quit()

# Vous pouvez ensuite sauvegarder ces informations dans un fichier CSV ou une base de données
