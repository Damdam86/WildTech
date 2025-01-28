import requests
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import concurrent.futures

NB_PAGES = 54
BASE_URL = "https://www.jaimelesstartups.fr/liste-des-startups-en-france"
PAGE_URL = "https://www.jaimelesstartups.fr/liste-des-startups-en-france/page/{}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
}

def get_startup_links_from_listing(page_number):
    """
    Récupère la liste des URLs de fiches startups sur la page page_number.
    """
    # Construit l'URL : 
    # 1) si page_number=1 => BASE_URL
    # 2) sinon => PAGE_URL.format(page_number)
    if page_number == 1:
        url = BASE_URL
    else:
        url = PAGE_URL.format(page_number)

    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Les articles sont dans <article class="tuile ...">
    #   <a href="https://www.jaimelesstartups.fr/xxxxx/">
    # On sélectionne la balise <article>, puis le <a> principal
    articles = soup.select("article.tuile div.tuileimage a")

    links = []
    for a in articles:
        href = a.get("href")
        if href:
            links.append(href)
    return links

def parse_startup_detail(url):
    """
    Parse la page de détail d’une startup : récupère nom, tags, description,
    site web, etc. Retourne un dict avec les champs désirés.
    """
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    data = {
        "url": url,
        "name": "",
        "tags": [],
        "date_info": "",  # ex: "Dernière mise à jour : ..."
        "description": "",
        "website": "",
        "social_links": [],
        # Champs optionnels : employés, fondateurs, levee
        "employees": "",
        "founders": "",
        "funding": ""
    }

    # --- Nom de la startup ---
    # <h1 class="titre">Monomeris Chemicals</h1>
    name_el = soup.select_one("div.col-12 h1.titre")
    if name_el:
        data["name"] = name_el.get_text(strip=True)

    # --- Récupération des tags ---
    # <a class="c" ...>#Startup </a><a class="t" ...>#B2B </a> etc.
    # On peut les sélectionner tous dans le même bloc "div.col-12 h1.titre" + ... 
    # Ou plus simplement : tous les <a class="c"> + <a class="t">
    tag_els = soup.select("div.col-12 a.c, div.col-12 a.t")
    tags = [el.get_text(strip=True) for el in tag_els]
    data["tags"] = tags  # liste

    # --- Date info ---
    # <span class="lu">Dernière mise à jour : 23/01/2025 ...</span>
    date_el = soup.select_one("span.lu")
    if date_el:
        data["date_info"] = date_el.get_text(strip=True)

    # --- Description ---
    # <p class="reponse-courte"> ... </p>
    desc_el = soup.select_one("p.reponse-courte")
    if desc_el:
        data["description"] = desc_el.get_text("\n", strip=True)

    # --- Site Web ---
    # <a class="urlwebsite" href="..." role="button"> → Monomeris Chemicals</a>
    website_el = soup.select_one("a.urlwebsite")
    if website_el:
        data["website"] = website_el.get("href", "")

    # --- Réseaux sociaux ---
    # <a class="urllinkedin" href="...">
    # On peut chercher tous les <a> dans ul.lien-startup
    social_container = soup.select_one("ul.lien-startup")
    if social_container:
        # ex: <a class="urllinkedin" ...>, <a class="urltwitter"> ...
        # On va prendre tout <a> et on identifie le type via la classe
        a_elems = social_container.select("a")
        for a in a_elems:
            href = a.get("href", "")
            classes = a.get("class", [])
            if any("urllinkedin" in c for c in classes):
                data["social_links"].append({"type": "linkedin", "url": href})
            elif any("urlfacebook" in c for c in classes):
                data["social_links"].append({"type": "facebook", "url": href})
            elif any("urltwitter" in c for c in classes):
                data["social_links"].append({"type": "twitter", "url": href})
            else:
                # Autre (instagram, etc.)
                data["social_links"].append({"type": "other", "url": href})

    # --- Employés / Fondateurs / Levée de fond ---
    # Le site ne semble pas avoir un bloc unique standardisé,
    # peut-être inexistant. Exemple d'extraction "ad hoc" :
    # (Ici on met juste des placeholders vides, vous pouvez adapter
    #  si vous voyez un pattern HTML spécifique pour "employés" ou "levée".)

    return data

def scrape_all_listings():
    """
    Récupère la liste de *tous* les liens startups (sur 54 pages),
    puis parse chaque startup en multithread.
    Retourne la liste des data.
    """
    # 1) Récupérer tous les liens
    all_links = []
    for page_num in tqdm(range(1, NB_PAGES + 1), desc="Liste pages"):
        links = get_startup_links_from_listing(page_num)
        all_links.extend(links)

    # Dédupliquer
    all_links = list(set(all_links))
    print(f"Total de startups trouvées : {len(all_links)}")

    # 2) Multithreading pour parser chaque fiche
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(parse_startup_detail, url): url for url in all_links}
        for future in tqdm(concurrent.futures.as_completed(future_to_url),
                           total=len(future_to_url),
                           desc="Parsing fiches"):
            url = future_to_url[future]
            try:
                data = future.result()
                results.append(data)
            except Exception as e:
                print(f"Erreur sur {url}: {e}")
    return results

def save_to_csv(all_data, filename="jaimelesstartups.csv"):
    """
    Sauvegarde la liste all_data (dicos) dans un CSV.
    """
    # On définit les colonnes
    fieldnames = [
        "url",
        "name",
        "tags",
        "date_info",
        "description",
        "website",
        "social_links",
        "employees",
        "founders",
        "funding",
    ]
    # Conversion / flatten si besoin (tags, social_links sont des listes)
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in all_data:
            # tags → ", ".join()
            if isinstance(row["tags"], list):
                row["tags"] = ", ".join(row["tags"])
            # social_links → JSON ou join
            if isinstance(row["social_links"], list):
                # On peut faire une concat "type=url" ou un JSON
                row["social_links"] = str(row["social_links"])
            writer.writerow(row)

def main():
    all_data = scrape_all_listings()
    save_to_csv(all_data)
    print("Scraping terminé ! Résultats dans jaimelesstartups.csv")

if __name__ == "__main__":
    main()
