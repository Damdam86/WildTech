import time
import csv
import json
from urllib.parse import urljoin
from tqdm import tqdm
from playwright.sync_api import sync_playwright

BASE_URL = "https://lehub.web.bpifrance.fr/search"
START_PAGE = 32  # Démarrer la pagination à la page 27
NB_PAGES = 60    # Jusqu'à la page 50, par exemple

def click_voir_tout(page):
    """
    Tente de cliquer sur le bouton "Voir tout" s'il existe.
    Retourne True si le clic a eu lieu, False sinon.
    """
    button = page.query_selector('button.ds-btn.ds-btn--secondary:has-text("Voir tout")')
    if button:
        button.click()
        time.sleep(2)  # On laisse la page charger
        return True
    return False

def set_hits_per_page(page, hits=18):
    """
    Sélectionne le nombre de résultats par page (6, 12, 18, etc.)
    dans le <select class="ais-HitsPerPage-select">.
    """
    try:
        page.select_option('select.ais-HitsPerPage-select', str(hits))
        time.sleep(2)  # On attend le rechargement
        return True
    except:
        return False

def get_company_links(page):
    """
    Récupère tous les <a> dont le href commence par "/startup/".
    Renvoie une liste d'URLs absolues.
    """
    elements = page.query_selector_all('a[href^="/startup/"]')
    links = []
    for el in elements:
        href = el.get_attribute("href")
        if href:
            full_url = urljoin("https://lehub.web.bpifrance.fr", href)
            links.append(full_url)
    return list(set(links))

def get_company_details(page, url):
    """
    Ouvre la page d'une startup et extrait les infos :
      - Nom, logo, année de création, employés, tags, description, site, réseaux sociaux,
      - Business model, région, département, ville, présence,
      - Montant levé, nombre d'investisseurs, tour max, historique (investments).
    """
    page.goto(url)
    page.wait_for_timeout(2000)  # Attendre un peu le chargement

    details = {
        "url": url,
        "name": "",
        "logo": "",
        "creation_year": "",
        "employees": "",
        "tags": [],
        "description": "",
        "site": "",
        "social_links": [],
        "business_model": "",
        "region": "",
        "department": "",
        "city": "",
        "presence": [],
        "amount_raised": "",
        "investors_count": "",
        "round_max": "",
        "investments": []
    }

    # --- Logo ---
    logo_el = page.query_selector('div.ant-col img.sc-fJYnPK')
    if logo_el:
        details["logo"] = logo_el.get_attribute("src")

    # --- Nom ---
    name_el = page.query_selector('h3.sc-eBZNlw')
    if name_el:
        details["name"] = name_el.text_content().strip()

    # --- "Crée en XXXX • XX employés" ---
    creation_el = page.query_selector('h5.sc-iNOOhx')
    if creation_el:
        text = creation_el.text_content().strip()
        parts = text.split('•')
        if len(parts) > 0:
            left = parts[0].replace("Crée en", "").strip()
            details["creation_year"] = left
        if len(parts) > 1:
            right = parts[1].replace("employés", "").strip()
            details["employees"] = right

    # --- Tags (#data, #geomarketing, etc.) ---
    tag_els = page.query_selector_all('span.ant-tag.ant-tag-has-color')
    for t in tag_els:
        txt = t.text_content().strip()
        details["tags"].append(txt)

    # --- Description ---
    desc_el = page.query_selector('p.sc-fBVmgO')
    if desc_el:
        details["description"] = desc_el.text_content().strip()

    # --- Site web ---
    site_el = page.query_selector('a.sc-iwqaRn')
    if site_el:
        details["site"] = site_el.get_attribute("href")

    # --- Réseaux sociaux ---
    social_container = page.query_selector('div.sc-dPFVfm')
    if social_container:
        social_links = social_container.query_selector_all('a')
        for sl in social_links:
            href = sl.get_attribute("href")
            if href:
                low_href = href.lower()
                if "linkedin" in low_href:
                    details["social_links"].append({"type": "linkedin", "url": href})
                elif "twitter" in low_href:
                    details["social_links"].append({"type": "twitter", "url": href})
                elif "facebook" in low_href:
                    details["social_links"].append({"type": "facebook", "url": href})
                else:
                    details["social_links"].append({"type": "other", "url": href})

    # --- Bloc "À propos" (business model, etc.) ---
    about_blocks = page.query_selector_all('div.sc-fncAUw.dCgAbG')
    for ab in about_blocks:
        label_el = ab.query_selector('h5.sc-eywEdf')
        if not label_el:
            continue
        label = label_el.text_content().strip().lower()
        link_els = ab.query_selector_all('a')
        values = [lk.text_content().strip() for lk in link_els]

        if "business model" in label:
            if values:
                details["business_model"] = ", ".join(values)
        elif "région" in label:
            if values:
                details["region"] = values[0]
        elif "département" in label:
            if values:
                details["department"] = values[0]
        elif "ville" in label:
            if values:
                details["city"] = values[0]
        elif "présence" in label:
            details["presence"] = values

    # --- Investissements ---
    investments_block = page.query_selector('div.sc-lkltAP.iFHIms')
    if investments_block:
        # Montant total, nb investisseurs, tour max
        info_els = investments_block.query_selector_all('div.sc-bqlKNb.ePOhDA')
        for info in info_els:
            label_el = info.query_selector('div.sc-dusUTO')
            value_el = info.query_selector('div.sc-dLlDCe')
            if label_el and value_el:
                lbl = label_el.text_content().strip().lower()
                val = value_el.text_content().strip()
                if "montant total" in lbl:
                    details["amount_raised"] = val
                elif "investisseur" in lbl:
                    details["investors_count"] = val
                elif "tour max" in lbl or "série" in lbl:
                    details["round_max"] = val

        # Historique d'investissement
        timeline_block = investments_block.query_selector('div.sc-fFDWmC')
        if timeline_block:
            tours = timeline_block.query_selector_all('div.sc-hXaEyf.klgMnI')
            for t in tours:
                date_el = t.query_selector('div.sc-ekhVBu')
                amount_el = t.query_selector('div.sc-ivWWxv')
                round_el = t.query_selector('div.sc-kWqmjU')
                inv_el = t.query_selector('div[style*="opacity: 1;"]')

                date_str = date_el.text_content().strip() if date_el else ""
                amt_str = amount_el.text_content().strip() if amount_el else ""
                r_str = round_el.text_content().strip() if round_el else ""
                inv_str = ""
                if inv_el:
                    inv_str = inv_el.text_content().strip().replace("\n", " ").strip()

                details["investments"].append({
                    "date": date_str,
                    "amount": amt_str,
                    "round": r_str,
                    "investors": inv_str
                })

    return details

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Mettez True si vous voulez en headless
        page = browser.new_page()

        # 1) Aller d'abord sur la page de recherche
        page.goto(BASE_URL)
        time.sleep(2)

        # 2) Cliquer sur "Voir tout" pour afficher le listing
        ok = click_voir_tout(page)
        if not ok:
            print("Bouton 'Voir tout' pas trouvé (étape initiale).")

        # 3) Sélectionner 18 fiches/page
        hits_selected = set_hits_per_page(page, hits=18)
        if not hits_selected:
            print("Impossible de sélectionner 18 sociétés/page (étape initiale).")

        # On commence la liste de tous les liens
        all_links = []

        # 4) Parcourir de la page 27 à NB_PAGES
        for i in range(START_PAGE, NB_PAGES + 1):
            print(f"\n==== PAGE {i} ====")
            # Construction de l'URL
            url = f"{BASE_URL}?page={i}"
            page.goto(url)
            time.sleep(2)

            # (Éventuellement, recliquer sur "Voir tout" si le site l'exige,
            #  mais on suppose ici qu'une fois cliqué, le mode est conservé.)
            #
            # ok2 = click_voir_tout(page)
            # if not ok2:
            #     print("Bouton 'Voir tout' pas trouvé sur cette page.")
            #
            # (Idem pour set_hits_per_page => si besoin, vous pouvez réappeler.)

            # 5) Récupérer les liens
            links = get_company_links(page)
            print(f"Liens récupérés sur la page {i} : {len(links)}")

            if len(links) == 0:
                print("Plus de résultats, on arrête la pagination.")
                break

            all_links.extend(links)

        # Dédupliquer
        all_links = list(set(all_links))
        print(f"\nTotal de liens uniques : {len(all_links)}")

        # 6) Écriture CSV
        csv_filename = "lehub_results3.csv"
        print(f"Enregistrement dans {csv_filename}...")

        fieldnames = [
            "url",
            "name",
            "logo",
            "creation_year",
            "employees",
            "tags",
            "description",
            "site",
            "social_links",
            "business_model",
            "region",
            "department",
            "city",
            "presence",
            "amount_raised",
            "investors_count",
            "round_max",
            "investments"
        ]

        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()

            count = 0
            print("Extraction des fiches...")

            for link in tqdm(all_links):
                try:
                    data = get_company_details(page, link)
                    # Convertir listes/dicts en textes
                    if isinstance(data["tags"], list):
                        data["tags"] = ", ".join(data["tags"])
                    if isinstance(data["presence"], list):
                        data["presence"] = ", ".join(data["presence"])
                    if isinstance(data["social_links"], list):
                        data["social_links"] = json.dumps(data["social_links"], ensure_ascii=False)
                    if isinstance(data["investments"], list):
                        data["investments"] = json.dumps(data["investments"], ensure_ascii=False)

                    writer.writerow(data)
                    count += 1

                    # Flush tous les 20
                    if count % 20 == 0:
                        f.flush()
                        print(f"{count} sociétés enregistrées (flush).")

                except Exception as e:
                    print(f"Erreur sur {link}: {e}")

        browser.close()
        print(f"Scraping terminé ! {count} sociétés sauvegardées dans {csv_filename}.")

if __name__ == "__main__":
    main()
