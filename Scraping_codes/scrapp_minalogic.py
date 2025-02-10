from playwright.sync_api import sync_playwright
import csv
from tqdm import tqdm
from urllib.parse import urljoin

BASE_URL = "https://www.minalogic.com/annuaire/"
PAGES = 29  # Nombre total de pages à scraper


def get_company_links(page, url):
    """Récupère les liens des entreprises sur une page donnée."""
    page.goto(url)
    links = []
    # On cherche tous les liens contenant '/annuaire/'
    elements = page.query_selector_all('a[href*="/annuaire/"]')
    for element in elements:
        href = element.get_attribute("href")
        if href:
            # Convertir en absolu :
            full_url = urljoin("https://www.minalogic.com", href)
            # Éviter la pagination (on ne veut pas /annuaire/page/xx)
            if "/annuaire/page/" not in full_url:
                links.append(full_url)
    return links


def get_company_details(page, company_url):
    """Récupère les détails d'une entreprise à partir de son URL."""
    page.goto(company_url)
    details = {
        'url': company_url,
        'logo': None,
        'name': None,
        'description': None,
        'infos': {},
        'contact': {}
    }
    
    # Logo
    logo_element = page.query_selector('div.nq-c-SingleHead-logo img')
    if logo_element:
        details['logo'] = logo_element.get_attribute("src")
    
    # Nom de la société
    name_element = page.query_selector('h1.nq-c-SingleHead-title')
    if name_element:
        details['name'] = name_element.text_content().strip()
    
    # Description
    description_element = page.query_selector('div.nq-c-SingleDetail')
    if description_element:
        details['description'] = description_element.text_content().strip()
    
    # Infos
    info_blocks = page.query_selector_all('div.nq-c-SingleInfo-block')
    for block in info_blocks:
        title_element = block.query_selector('div.nq-c-SingleInfo-block-title')
        if title_element:
            title_text = title_element.text_content().strip()
            if "Infos" in title_text:
                items = block.query_selector_all('div.nq-c-SingleInfo-block-detail-item')
                for item in items:
                    key_el = item.query_selector('span')
                    if key_el:
                        key = key_el.text_content().strip()
                        # Retirer la clé du texte global
                        item_text = item.text_content().strip()
                        value = item_text.replace(key, '').strip()
                        details['infos'][key] = value
            elif "Contact" in title_text:
                items = block.query_selector_all('div.nq-c-SingleInfo-block-detail-item')
                for item in items:
                    key_el = item.query_selector('span')
                    if key_el:
                        key = key_el.text_content().strip()
                        item_text = item.text_content().strip()
                        value = item_text.replace(key, '').strip()
                        details['contact'][key] = value

    return details


def main():
    with sync_playwright() as p:
        # Lancer le navigateur
        browser = p.chromium.launch(headless=True)  # headless=False pour déboguer
        page = browser.new_page()
        
        # Étape 1 : Récupérer tous les liens des entreprises sur chaque page
        print("Récupération des liens des entreprises...")
        all_links = []
        for i in range(1, PAGES + 1):
            if i == 1:
                # Page 1 est BASE_URL
                url = BASE_URL
            else:
                url = f"{BASE_URL}page/{i}/"
            links = get_company_links(page, url)
            all_links.extend(links)
            print(f"Page {i} traitée. {len(links)} liens trouvés.")

        # Supprimer les doublons
        all_links = list(set(all_links))
        print(f"{len(all_links)} liens d'entreprises récupérés (uniques).")

        # Étape 2 : Récupérer les détails de chaque entreprise
        print("Récupération des détails des entreprises...")
        all_details = []
        for link in tqdm(all_links, desc="Entreprises traitées"):
            try:
                details = get_company_details(page, link)
                all_details.append(details)
            except Exception as e:
                print(f"Erreur pour {link}: {e}")
        
        # Étape 3 : Sauvegarder les résultats dans un fichier CSV
        print("Sauvegarde des données dans un fichier CSV...")
        csv_filename = 'entreprises.csv'
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['url', 'logo', 'name', 'description', 'infos', 'contact']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for details in all_details:
                # Convertir les dictionnaires en chaînes de caractères
                details['infos'] = str(details['infos'])
                details['contact'] = str(details['contact'])
                writer.writerow(details)
        
        print(f"Scraping terminé. Les données ont été sauvegardées dans '{csv_filename}'.")
        browser.close()


if __name__ == "__main__":
    main()
