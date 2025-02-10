import time
import csv
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Pour les "explicit wait"
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

URL = "https://www.maddyness.com/2024/11/15/maddymoney-184-millions-deuros-leves-cette-semaine-par-les-startups-francaises/"

def click_cookie_banner(driver):
    """
    Si un bandeau cookies apparaît, on clique sur "Accepter" ou "Tout accepter".
    Adaptez le sélecteur selon la structure réelle.
    """
    try:
        # Exemple fictif : un bouton //button[contains(text(),'Accepter')]
        accept_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Accepter')]"))
        )
        accept_button.click()
        time.sleep(2)
    except:
        # S’il n’y a pas de bandeau, on passe
        pass

def get_financements_data(html):
    """
    Parcourt le HTML final, cherche <div class="financements__item">,
    et récupère hashtag, name, amount, investors, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("div.financements__item")

    data_list = []
    for item in items:
        # Hashtag
        hashtag_el = item.select_one("div.financements__hashtag span")
        hashtag = hashtag_el.get_text(strip=True) if hashtag_el else ""

        # Nom
        name_el = item.select_one("a.financements__name")
        name = name_el.get_text(strip=True) if name_el else ""
        name_link = name_el.get("href") if name_el else ""

        # Montant
        amount_el = item.select_one("div.financements__amount")
        amount = amount_el.get_text(strip=True) if amount_el else ""

        # Investisseurs
        investors_els = item.select("div.financements__text a")
        investors_list = [a.get_text(strip=True) for a in investors_els]
        investors = ", ".join(investors_list)

        data_list.append({
            "hashtag": hashtag,
            "name": name,
            "name_link": name_link,
            "amount": amount,
            "investors": investors
        })

    return data_list

def main():
    options = Options()
    # options.add_argument("--headless")  # si vous voulez en mode invisible
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(URL)
        time.sleep(2)
        
        # 1) Tenter de cliquer sur un éventuel bandeau cookies
        click_cookie_banner(driver)

        # 2) Attendre que les éléments "financements__item" soient présents
        wait = WebDriverWait(driver, 15)  # on attend max 15s
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.financements__item"))
        )

        # 3) Récupérer le DOM final
        html = driver.page_source

    finally:
        driver.quit()

    # 4) Extraire les financements
    financements_data = get_financements_data(html)

    # 5) Affichage + CSV
    print(f"Nombre de financements trouvés : {len(financements_data)}")
    for f in financements_data:
        print(f"- {f['hashtag']} | {f['name']} | {f['amount']} | {f['investors']}")

    csv_filename = "maddymoney_financements_15_11_24.csv"
    fieldnames = ["hashtag","name","name_link","amount","investors"]
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in financements_data:
            writer.writerow(row)

    print(f"Les données ont été enregistrées dans {csv_filename}.")

if __name__ == "__main__":
    main()
