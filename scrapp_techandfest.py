from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json


class TechFestScraper:
    def __init__(self):
        self.driver = self.setup_driver()
        self.results = []

    def setup_driver(self):
        """Configure et initialise le driver Selenium."""
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver

    def get_card_links(self):
        """Récupère les liens des cartes de la page courante."""
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.itemcontent.clickable.card"))
            )
            cards = self.driver.find_elements(By.CSS_SELECTOR, "a.itemcontent.clickable.card")
            links = [card.get_attribute("href") for card in cards]
            return links
        except TimeoutException:
            print("Erreur : Les cartes n'ont pas pu être chargées.")
            return []

    def scrape_card_details(self, link):
        """Extrait les informations détaillées d'une société."""
        driver = self.setup_driver()
        try:
            driver.get(link)
            time.sleep(3)  # Attendre que la page se charge

            name = self.get_text(driver, "section h1 .fieldtext", multiple=False) or "Nom non disponible"
            description = self.get_text(driver, "section .group-field .fieldtext", multiple=False) or "Description non disponible"
            keywords = self.get_text(driver, ".themeslist .eventtheme .fieldtext", multiple=True)
            website = self.get_text(driver, "span.label", multiple=False) or "Site web non disponible"
            logo = self.get_attribute(driver, "img.logo.logo", "src") or "Logo non disponible"
            tags = self.get_text(driver, ".tagslist-items .tag", multiple=True)
            stand_number = self.get_text(driver, ".standnumber .fieldtext", multiple=False) or "Emplacement non spécifié"

            return {
                "nom": name,
                "description": description,
                "mots_cles": keywords or [],
                "site_web": website,
                "logo": logo,
                "tags": tags or [],
                "emplacement": stand_number,
            }
        except Exception as e:
            print(f"Erreur lors de l'extraction des données pour {link}: {e}")
            return None
        finally:
            driver.quit()

    def get_text(self, driver, selector, multiple=False):
        """Extrait le texte d'un ou plusieurs éléments."""
        try:
            if multiple:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                return [elem.text.strip() for elem in elements]
            else:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                return element.text.strip()
        except NoSuchElementException:
            return [] if multiple else None

    def get_attribute(self, driver, selector, attribute):
        """Extrait un attribut d'un élément."""
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            return element.get_attribute(attribute)
        except NoSuchElementException:
            return None

    def collect_all_card_links(self):
        """Collecte tous les liens des cartes des pages déjà chargées."""
        print("Laissez-moi savoir une fois que toutes les pages sont chargées manuellement.")
        input("Appuyez sur Entrée pour commencer à collecter les liens des cartes...")
        all_links = []
        while True:
            card_links = self.get_card_links()
            all_links.extend(card_links)

            user_input = input("Passez à la page suivante manuellement, puis appuyez sur Entrée (ou tapez 'exit' pour terminer) : ")
            if user_input.lower() == "exit":
                break
        print(f"Nombre total de liens collectés : {len(all_links)}")
        return all_links

    def scrape_all_cards_multithread(self, links, max_workers=6):
        """Scrape toutes les cartes en utilisant des threads."""
        print(f"Scraping de {len(links)} cartes avec {max_workers} threads...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.scrape_card_details, link) for link in links]

            for future in as_completed(futures):
                details = future.result()
                if details:
                    self.results.append(details)

        self.save_results()

    def save_results(self):
        """Sauvegarde les résultats dans un fichier JSON."""
        with open("tech_fest_data.json", "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4)
        print("Résultats sauvegardés dans 'tech_fest_data.json'.")

    def close(self):
        """Ferme le driver Selenium."""
        self.driver.quit()


if __name__ == "__main__":
    scraper = TechFestScraper()
    try:
        card_links = scraper.collect_all_card_links()
        scraper.scrape_all_cards_multithread(card_links, max_workers=6)
    except KeyboardInterrupt:
        print("Scraping interrompu par l'utilisateur.")
    finally:
        scraper.save_results()
        scraper.close()
