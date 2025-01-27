import time
import csv
from tqdm import tqdm
from bs4 import BeautifulSoup

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Nombre total de pages
NB_PAGES = 29

BASE_URL = "https://www.minalogic.com/annuaire"
PAGE_URL = "https://www.minalogic.com/annuaire/page/{}"


def init_driver(headless=True):
    """
    Initialise un driver Chrome Selenium. Si headless=True, pas d'interface graphique.
    """
    from selenium.webdriver.chrome.options import Options
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
    # Installe le bon ChromeDriver si pas déjà présent
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def get_company_links_selenium(driver, page_number):
    """
    Charge la page (page_number) dans Selenium, attend (si nécessaire),
    parse avec BeautifulSoup, et récupère les liens.
    """
    if page_number == 1:
        url = BASE_URL
    else:
        url = PAGE_URL.format(page_number)

    driver.get(url)
    # On peut attendre un peu que le JS charge les éléments si nécessaire
    time.sleep(2)  # Ajustez si le site est lent

    # Récupérer la page_source
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # Repérer les blocs de fiches. Souvent : <li class="nq-c-Cards-list-item"><a class="nq-c-Card" href="...">
    cards = soup.select("li.nq-c-Cards-list-item a.nq-c-Card")
    links = [card.get('href') for card in cards if card.get('href')]
    return links


def parse_company_page(url, driver=None):
    """
    Ouvre la page détaillée d'une entreprise via Selenium,
    parse le HTML avec BeautifulSoup, et retourne les infos.
    
    Option : vous pouvez utiliser le driver Selenium existant.
    """
    if driver is None:
        # Cas où on crée un driver juste pour cette page
        driver_local = init_driver(headless=True)
        driver_local.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver_local.page_source, "html.parser")
        driver_local.quit()
    else:
        # Réutilise le driver existant (moins efficace si on doit enchaîner beaucoup d'URL)
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")

    # Nom
    name_tag = soup.select_one("h1.nq-c-SingleHead-title")
    name = name_tag.get_text(strip=True) if name_tag else ""

    # Logo
    logo_tag = soup.select_one("div.nq-c-SingleHead-logo img")
    logo_url = logo_tag.get("src") if logo_tag else ""

    # Description
    desc_tag = soup.select_one("div.nq-c-SingleDetail.nq-c-Wysiwyg")
    description = desc_tag.get_text("\n", strip=True) if desc_tag else ""

    # Infos (premier bloc)
    info_blocks = soup.select("div.nq-c-SingleInfo-block")
    infos_dict = {}
    if len(info_blocks) > 0:
        detail_items = info_blocks[0].select(".nq-c-SingleInfo-block-detail-item")
        for item in detail_items:
            label_tag = item.select_one("span")
            if not label_tag:
                continue
            label = label_tag.get_text(strip=True)
            value_tag = item.select_one("p")
            if value_tag:
                value_text = value_tag.get_text(", ", strip=True)
            else:
                value_text = item.get_text(strip=True)
            infos_dict[label] = value_text

    # Contact (deuxième bloc)
    contact_dict = {}
    if len(info_blocks) > 1:
        detail_items = info_blocks[1].select(".nq-c-SingleInfo-block-detail-item")
        for item in detail_items:
            label_tag = item.select_one("span")
            if not label_tag:
                continue
            label = label_tag.get_text(strip=True)
            value_tag = item.select_one("p")
            if value_tag:
                value_text = value_tag.get_text("\n", strip=True)
            else:
                value_text = item.get_text(strip=True)
            contact_dict[label] = value_text

        # Liens (site web, twitter, linkedin...)
        links_tags = info_blocks[1].select(".nq-c-SingleInfo-block-links a")
        extra_links = {}
        for link in links_tags:
            text = link.get_text(strip=True)
            href = link.get("href")
            if not text:
                # On essaie de deviner
                if "twitter" in href.lower():
                    text = "Twitter"
                elif "linkedin" in href.lower():
                    text = "LinkedIn"
                else:
                    text = "Autre"
            extra_links[text] = href
        contact_dict["Liens"] = extra_links

    data = {
        "url": url,
        "name": name,
        "logo_url": logo_url,
        "description": description,
        "infos": infos_dict,
        "contact": contact_dict,
    }

    return data


def main():
    # 1) Lancement du driver
    driver = init_driver(headless=True)

    # 2) Récupération de tous les liens d'entreprises
    all_links = []
    for page_number in tqdm(range(1, NB_PAGES + 1), desc="Pages"):
        links = get_company_links_selenium(driver, page_number)
        all_links.extend(links)

    print(f"Nombre total de fiches récupérées : {len(all_links)}")
    if not all_links:
        print("Aucun lien trouvé : vérifier si le site n'est pas bloqué ou a changé de structure.")
        driver.quit()
        return

    # 3) Pour chaque lien, on ouvre la page et on parse
    all_data = []
    for link in tqdm(all_links, desc="Fiches"):
        try:
            data = parse_company_page(link, driver=driver)
            all_data.append(data)
        except Exception as e:
            print(f"Erreur en traitant {link}: {e}")

    # On peut fermer le driver ici
    driver.quit()

    # 4) Sauvegarde en CSV
    csv_filename = "minalogic_annuaire.csv"
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        header = ["url", "name", "logo_url", "description", "infos", "contact"]
        writer.writerow(header)

        for item in all_data:
            writer.writerow([
                item["url"],
                item["name"],
                item["logo_url"],
                item["description"],
                str(item["infos"]),
                str(item["contact"])
            ])

    print(f"Scraping terminé ! Résultats sauvegardés dans {csv_filename}")


if __name__ == "__main__":
    main()
