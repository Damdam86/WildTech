import os
import json
import time
from math import ceil
from concurrent.futures import ProcessPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, NoSuchElementException
)
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager



# ========================
#     PARTIE SELENIUM
# ========================

def create_driver():
    """Crée et configure le driver Chrome (headless)."""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
    # Désactivation du chargement des images
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def scroll_down(driver):
    """
    Fait défiler la page entière pour s'assurer que tout est chargé
    """
    try:
        # Scroll initial
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        # Scroll progressif
        total_height = driver.execute_script("return document.body.scrollHeight")
        for height in range(0, total_height, 100):
            driver.execute_script(f"window.scrollTo(0, {height});")
            time.sleep(0.1)
            
        # Scroll final
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
    except Exception as e:
        print(f"Erreur de scroll: {str(e)}")


def maybe_click_see_more(driver):
    """Tente de cliquer sur un éventuel bouton 'Voir plus'."""
    try:
        button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Voir plus')]"))
        )
        if button:
            button.click()
            time.sleep(1)
    except:
        pass


def extract_investments(driver):
    """
    Récupère la partie 'Investissements' et 'Historique des tours'
    Retourne un dict {"investment": {...}, "funding_rounds": [...]}
    """
    result = {
        "investment": {},
        "funding_rounds": []
    }
    
    try:
        # 1. S'assurer que la page est bien chargée
        scroll_down(driver)
        time.sleep(3)  # Attente plus longue pour le chargement complet
        
        # 2. Trouver la section des investissements
        try:
            # Attendre explicitement que tous les h4 soient chargés
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "h4"))
            )
            
            # Trouver tous les h4 et chercher celui qui contient "Investissements"
            all_h4s = driver.find_elements(By.TAG_NAME, "h4")
            invest_h4 = None
            for h4 in all_h4s:
                if "Investissements" in h4.text:
                    invest_h4 = h4
                    break
                    
            if not invest_h4:
                print("Section Investissements non trouvée")
                return result
                
            # Remonter à la card parent
            card_body = invest_h4.find_element(By.XPATH, "./ancestor::div[contains(@class, 'ant-card-body')]")
            
            # 3. Extraire les informations de base (montant total, investisseurs, etc.)
            info_divs = card_body.find_elements(By.XPATH, ".//div[contains(@class, 'sc-bqlKNb') or contains(@class, 'ePOhDA')]")
            
            for div in info_divs:
                try:
                    # Trouver tous les div enfants
                    child_divs = div.find_elements(By.XPATH, "./div")
                    if len(child_divs) >= 2:
                        label = child_divs[0].text.strip()
                        value = child_divs[1].text.strip()
                        if label and value:
                            result["investment"][label] = value
                except Exception as e:
                    print(f"Erreur extraction info: {str(e)}")
                    continue
            
            # 4. Extraire l'historique des tours
            # Chercher les blocs de tours avec une approche plus générique
            round_blocks = card_body.find_elements(
                By.XPATH,
                ".//div[contains(@class, 'sc-hXaEyf') or contains(@class, 'klgMnI')]"
            )
            
            for block in round_blocks:
                round_data = {
                    "date": "",
                    "amount": "",
                    "round_type": "",
                    "investors": []
                }
                
                # Extraire tous les div enfants directs
                child_divs = block.find_elements(By.XPATH, "./div")
                
                for div in child_divs:
                    text = div.text.strip()
                    if not text:
                        continue
                        
                    # Identifier le type d'information basé sur le format
                    if text.lower().endswith('€') or text.endswith('$'):
                        round_data["amount"] = text
                    elif any(month in text.lower() for month in ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre']):
                        round_data["date"] = text
                    elif text.upper() == text:  # Si le texte est tout en majuscules
                        round_data["round_type"] = text
                    
                # Chercher les investisseurs dans un div contenant des liens
                try:
                    investors_divs = block.find_elements(
                        By.XPATH,
                        ".//div[.//a]"
                    )
                    for div in investors_divs:
                        investor_links = div.find_elements(By.TAG_NAME, "a")
                        investors = [link.text.strip().rstrip(',') for link in investor_links if link.text.strip()]
                        if investors:
                            round_data["investors"].extend(investors)
                except Exception as e:
                    print(f"Erreur extraction investisseurs: {str(e)}")
                
                # Ajouter le tour seulement s'il contient des données
                if any(v for v in round_data.values() if v):
                    result["funding_rounds"].append(round_data)
                    
        except Exception as e:
            print(f"Erreur dans la recherche de la section: {str(e)}")
            
    except Exception as e:
        print(f"Erreur générale: {str(e)}")
        
    return result


def scrape_one_page(link):
    """Ouvre le driver, scrape le lien, retourne data."""
    driver = create_driver()
    data = {"url": link}
    try:
        driver.get(link)
        # Attendre un h3.sc-eBZNlw (ex: le nom)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h3.sc-eBZNlw"))
        )

        # Petit scroll + possible bouton 'Voir plus'
        scroll_down(driver)
        maybe_click_see_more(driver)

        # name
        try:
            data["name"] = driver.find_element(By.CSS_SELECTOR, "h3.sc-eBZNlw").text.strip()
        except:
            data["name"] = None

        # description
        try:
            data["description"] = driver.find_element(By.CSS_SELECTOR, "p.sc-fBVmgO").text.strip()
        except:
            data["description"] = None

        # logo
        try:
            logo_elem = driver.find_element(By.CSS_SELECTOR, "img.sc-fJYnPK")
            data["logo"] = logo_elem.get_attribute("src")
        except:
            data["logo"] = None

        # hashtags
        data["hashtags"] = []
        hashtag_elems = driver.find_elements(By.CSS_SELECTOR, "span.ant-tag")
        for tag in hashtag_elems:
            t = tag.text.strip()
            if t.startswith("#"):
                data["hashtags"].append(t)

        # website
        try:
            site_btn = driver.find_element(By.CSS_SELECTOR, "a.sc-iwqaRn")
            data["website"] = site_btn.get_attribute("href")
        except:
            data["website"] = None

        # about
        data["about"] = {}
        about_sections = driver.find_elements(By.CSS_SELECTOR, "div.sc-fncAUw.dCgAbG")
        for block in about_sections:
            try:
                section_title = block.find_element(By.CSS_SELECTOR, "h5.sc-eywEdf").text.strip()
                items = block.find_elements(By.CSS_SELECTOR, "div.sc-frfUUU a")
                vals = [it.text.strip() for it in items if it.text.strip()]
                data["about"][section_title] = vals
            except:
                pass

        # investissements
        investments = extract_investments(driver)
        data["investment"] = investments["investment"]
        data["funding_rounds"] = investments["funding_rounds"]

    except (TimeoutException, WebDriverException) as e:
        data["error"] = str(e)
    except Exception as e:
        data["error"] = f"Unexpected error: {e}"
    finally:
        driver.quit()

    return data


# ========================
#   PARTIE MULTICOEUR
# ========================

def chunk_list(lst, chunk_size):
    """Générateur pour découper une liste en paquets de taille chunk_size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]


def save_partial(data, outfile="bpifrance_partial.json"):
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"--- Sauvegarde partielle : {len(data)} éléments ---")


def main():
    input_file = "bpifrance_startups_links.json"
    if not os.path.exists(input_file):
        print(f"Fichier introuvable : {input_file}")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        all_links = json.load(f)

    print(f"=== Nombre total de liens : {len(all_links)} ===")

    # Paramètres
    CHUNK_SIZE = 3
    NB_WORKERS = 4  # Ajustez selon votre CPU

    # On découpe en chunks
    chunks = list(chunk_list(all_links, CHUNK_SIZE))
    print(f"=== On traite {len(chunks)} chunks de taille ~{CHUNK_SIZE} ===")

    all_results = []

    with ProcessPoolExecutor(max_workers=NB_WORKERS) as executor:
        # On lance le scraping en parallèle, chunk par chunk
        futures = {}
        for idx, chunk_links in enumerate(chunks, start=1):
            # Soumettre la tâche : scrape un chunk
            fut = executor.submit(scrape_chunk_of_links, chunk_links, idx)
            futures[fut] = idx

        for fut in as_completed(futures):
            chunk_idx = futures[fut]
            try:
                chunk_data = fut.result()  # list de dict
                all_results.extend(chunk_data)
                print(f"[Chunk {chunk_idx}] OK - {len(chunk_data)} résultats. Total : {len(all_results)}")
            except Exception as e:
                print(f"[Chunk {chunk_idx}] ERREUR: {e}")

            # Sauvegarde partielle après chaque chunk
            save_partial(all_results, outfile="bpifrance_partial.json")

    # Sauvegarde finale
    outfile = "bpifrance_startups_data.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)
    print(f"=== FIN : {len(all_results)} total => {outfile} ===")


# ========================
#   WORKER DE CHUNK
# ========================

def scrape_chunk_of_links(list_of_links, chunk_id):
    """
    Fonction qui s'exécute dans un process séparé.
    On ouvre/ferme un driver par item (ou vous pouvez en ouvrir 1 pour tout le chunk).
    """
    results = []
    for i, link in enumerate(list_of_links, start=1):
        print(f"[Chunk {chunk_id}] scrape {i}/{len(list_of_links)} : {link}")
        data = scrape_one_page(link)
        results.append(data)
    return results


if __name__ == "__main__":
    main()
