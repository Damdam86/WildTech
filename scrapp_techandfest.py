import time
import json
from functools import wraps

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager


def retry_stale(max_tries=2, sleep_sec=1):
    """
    Décorateur pour retenter l'opération quand on rencontre
    une StaleElementReferenceException (site très dynamique).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except StaleElementReferenceException as e:
                    if attempt == max_tries - 1:
                        raise
                    print(f"[retry_stale] StaleElementReferenceException, on retente... (tentative {attempt+1}/{max_tries})")
                    time.sleep(sleep_sec)
        return wrapper
    return decorator


class TechFestScraper:
    def __init__(self):
        self.base_url = "https://www.tech-fest.fr/fr/content/exposants"
        self.driver = self.setup_driver()
        self.results = []

    def setup_driver(self):
        """Configure et initialise le driver Selenium."""
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # User-agent réaliste pour éviter les blocages
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
             AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/90.0.4430.93 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver

    def get_card_links(self):
        """
        Récupère les liens de cartes (URL en texte) visibles dans la page actuelle.
        Si pas de cartes présentes, renvoie [].
        """
        try:
            # Attendre que la première carte soit présente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.itemcontent.clickable.card"))
            )
            cards = self.driver.find_elements(By.CSS_SELECTOR, "a.itemcontent.clickable.card")
            links = [card.get_attribute("href") for card in cards]
            print(f"{len(links)} liens récupérés.")
            return links
        except Exception as e:
            print(f"Erreur lors de la récupération des liens : {e}")
            return []

    @retry_stale(max_tries=2, sleep_sec=2)
    def scrape_card_details(self, link):
        """Extrait les informations détaillées d'une société."""
        print(f"Analyse du lien : {link}")
        self.driver.get(link)
        time.sleep(2)  # Attendre un peu pour que la page (re)charge

        try:
            # Attendre qu'un élément du bloc description apparaisse
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section.group-field.group-field .fieldtext"))
            )

            # Nom
            name = self.get_text("section h1 .fieldtext", multiple=False) or "Nom non disponible"
            # Description (2ème bloc group-field, si c'est cohérent)
            description = self.get_text("section.group-field.group-field:nth-of-type(2) .fieldtext", multiple=False)
            if not description:
                description = "Description non disponible"
            # Mots-clés
            keywords = self.get_text(".themeslist .eventtheme span", multiple=True)

            # Liens sociaux / site web
            social_links_raw = self.driver.find_elements(By.CSS_SELECTOR, ".sociallinks .sociallink .label")
            social_links = [sl.text.strip() for sl in social_links_raw if sl.text.strip()]
            print(f"DEBUG social_links = {social_links}")
            website = next((l for l in social_links if l.startswith("http")), "Site web non disponible")

            # Logo
            logo = self.get_attribute("img.logo.logo", "src") or "Logo non disponible"

            # Tags
            tags = self.get_text(".tagslist-items .tag", multiple=True)

            # Emplacement
            stand_number = self.get_text(".standnumber .fieldtext", multiple=False) or "Emplacement non spécifié"

            return {
                "nom": name,
                "description": description,
                "mots_cles": keywords or [],
                "site_web": website,
                "social_links": social_links,
                "logo": logo,
                "tags": tags or [],
                "emplacement": stand_number,
            }
        except Exception as e:
            print(f"Erreur lors de l'extraction des données : {e}")
            return {
                "nom": "Erreur",
                "description": "Erreur lors de l'extraction",
                "mots_cles": [],
                "site_web": None,
                "social_links": [],
                "logo": None,
                "tags": [],
                "emplacement": None,
            }

    def get_text(self, selector, multiple=False):
        """Extrait le texte d'un (ou plusieurs) élément(s) via .text."""
        try:
            if multiple:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                return [elem.text.strip() for elem in elements if elem.text.strip()]
            else:
                elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                return elem.text.strip()
        except NoSuchElementException:
            return [] if multiple else None

    def get_attribute(self, selector, attribute):
        """Extrait la valeur d'un attribut pour un sélecteur donné."""
        try:
            elem = self.driver.find_element(By.CSS_SELECTOR, selector)
            return elem.get_attribute(attribute)
        except NoSuchElementException:
            return None

    def scrape_all_pages_param(self, total_pages=9):
        """
        Méthode 1: tente la pagination paramétrique (?page=2, etc.)
        tout en dédupliquant les liens déjà vus.
        """
        visited_links = set()

        for page in range(1, total_pages + 1):
            print(f"Analyse de la page {page} (paramètre ?page={page})...")
            self.driver.get(f"{self.base_url}?page={page}")
            time.sleep(2)  # Laisser un peu de temps pour charger la page

            card_links = self.get_card_links()  # tous les liens trouvés
            new_links = [link for link in card_links if link not in visited_links]
            print(f"{len(new_links)} nouveaux liens non déjà visités.")

            # Si on récupère 0 nouveau lien, ça veut souvent dire que le site renvoie la même liste
            if not new_links:
                print(f"Aucun nouveau lien, on peut stopper à la page {page}.")
                break

            for link in new_links:
                visited_links.add(link)
                details = self.scrape_card_details(link)
                if details:
                    self.results.append(details)

            self.save_results()
            print(f"Page {page} terminée.\n")

    def scroll_infinite(self, max_scroll=10, pause=2):
        """
        Méthode utilitaire pour scroller vers le bas de la page plusieurs fois,
        utile si le site charge dynamiquement les exposants au scroll.
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(max_scroll):
            # Scroller tout en bas
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            # Si le scroll ne grandit plus, on arrête
            if new_height == last_height:
                break
            last_height = new_height

    def scrape_infinite_scroll(self):
        """
        Méthode 2: si la liste d'exposants est chargée en continu quand on scrolle,
        on scrolle et on récupère tous les liens en fin de scroll.
        """
        self.driver.get(self.base_url)
        time.sleep(2)

        # On scrolle plusieurs fois pour forcer le chargement
        self.scroll_infinite(max_scroll=10, pause=2)

        # Maintenant on récupère tous les liens de cartes
        card_links = self.get_card_links()
        for link in card_links:
            details = self.scrape_card_details(link)
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
        # Choisir la méthode de scraping
        # 1) Avec pagination paramétrique :
        scraper.scrape_all_pages_param(total_pages=9)

        # 2) Ou avec scroll infini :
        # scraper.scrape_infinite_scroll()

    except KeyboardInterrupt:
        print("Scraping interrompu par l'utilisateur.")
    finally:
        scraper.save_results()
        scraper.close()
