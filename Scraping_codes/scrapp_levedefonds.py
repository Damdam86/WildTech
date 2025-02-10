import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Pour les waits explicites
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scrape_funds_on_current_page(driver):
    """
    Scrape tous les fund-items affichés sur la page actuelle.
    Retourne une liste de dictionnaires : [ {date, name, sector, amount, status, investors}, ... ]
    """
    data = []

    # Attendre jusqu'à ce que tous les .fund-item soient présents (sauf le "Load more")
    wait = WebDriverWait(driver, 10)
    fund_items = wait.until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.fund-item:not(.fund-item--loadmore)"))
    )

    for item in fund_items:
        try:
            # date
            date_el = item.find_element(By.CLASS_NAME, "fund-item__date")
            date_text = date_el.text.strip() if date_el else ""

            # nom + secteur
            # => <div class="fund-item__title"><h3>Robeauté</h3><span>Medtech</span></div>
            title_div = item.find_element(By.CLASS_NAME, "fund-item__title")
            name_el = title_div.find_element(By.TAG_NAME, "h3")
            name_text = name_el.text.strip() if name_el else ""

            # secteur => le <span> juste après h3
            sector_el = title_div.find_element(By.TAG_NAME, "span")
            sector_text = sector_el.text.strip() if sector_el else ""

            # montant
            amount_el = item.find_element(By.CLASS_NAME, "fund-item__fund")
            amount_text = amount_el.text.strip() if amount_el else ""

            # statut (Série A, Série B, Amorçage, etc.)
            status_el = item.find_element(By.CLASS_NAME, "fund-item__status")
            status_text = status_el.text.strip() if status_el else ""

            # investisseurs => dans div.fund-item__investors
            investors_div = item.find_element(By.CLASS_NAME, "fund-item__investors")
            # on récupère éventuellement tous les .investors__thumbnail ou .investors__thumbnail__preview
            investor_spans = investors_div.find_elements(By.CSS_SELECTOR, ".investors__thumbnail__preview, .investors__thumbnail")
            # on concatène leurs text
            investor_names = [inv.text.strip() for inv in investor_spans if inv.text.strip()]
            investors_text = ", ".join(investor_names)

            data.append({
                "date": date_text,
                "name": name_text,
                "sector": sector_text,
                "amount": amount_text,
                "status": status_text,
                "investors": investors_text
            })

        except Exception as e:
            print(f"Erreur lors de l'extraction : {e}")

    return data

def click_next_page(driver, page_number):
    """
    Tente de cliquer sur la pagination <label for="funds-X"> pour charger la page suivante.
    Par exemple, pour page_number=2, on clique sur input#funds-2
    """
    try:
        time.sleep(1)
        input_radio = driver.find_element(By.ID, f"funds-{page_number}")
        driver.execute_script("arguments[0].click();", input_radio)
        time.sleep(3)  # Laisser la page se recharger
        return True
    except Exception as e:
        print(f"Erreur lors du passage à la page {page_number} : {e}")
        return False

def main():
    # URL de la page
    url = "https://www.maddyness.com/levees-de-fonds/"
    
    # Initialiser Selenium (Chrome)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    try:
        driver.get(url)
        time.sleep(3)  # Attendre le chargement initial

        all_data = []
        # On suppose qu'on veut parcourir ~10 pages (ou plus si la pagination va jusqu'à funds-10)
        for page_number in range(1, 11):
            print(f"Scraping page {page_number}...")
            # Scraper la page courante
            page_data = scrape_funds_on_current_page(driver)
            all_data.extend(page_data)

            # Tenter de passer à la page suivante
            # - Si page_number == 10 => on arrête
            if page_number < 10:
                success = click_next_page(driver, page_number + 1)
                if not success:
                    break

        # Sauvegarde en CSV
        csv_filename = "levees_de_fonds.csv"
        fieldnames = ["date","name","sector","amount","status","investors"]
        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            for row in all_data:
                writer.writerow(row)

        print(f"Scraping terminé. {len(all_data)} lignes collectées. Voir {csv_filename}.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
