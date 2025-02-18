import os
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import html, dcc
from flask_caching import Cache
from dash_app.utils.data_loader import get_dataframe
from dash_app.utils.preprocessing import preprocess_societe, preprocess_financements, filter_societe
from flask_caching import Cache

# Création de l'app Dash
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)
server = app.server  # Pour gunicorn

# Cache simple en mémoire (optionnel)
cache = Cache(app.server, config={"CACHE_TYPE": "simple"})
TIMEOUT = None  # ou un nombre de secondes si vous voulez un TTL

cache.clear()  

################################################
# 1) Chargement unique des CSV et prétraitements
################################################

@cache.memoize(timeout=TIMEOUT)
def load_data_once():
    """Charge les CSV en mémoire (une seule fois)."""
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")

    df_soc = pd.read_csv(os.path.join(base_path, "societes.csv"), low_memory=False)
    df_fin = pd.read_csv(os.path.join(base_path, "financements.csv"), low_memory=False)
    df_per = pd.read_csv(os.path.join(base_path, "personnes.csv"), low_memory=False)

    return df_soc, df_fin, df_per

df_societe_global, df_financements_global, df_personnes_global = load_data_once()

###############################################
# 2) Fonctions utilitaires de prétraitement
###############################################
def preprocess_societe(df):
    df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
    df["annee_creation"] = df["date_creation_def"].dt.year
    return df

def preprocess_financements(df):
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0)
    return df

def filter_societe(df, sector, effectif, year_range, year_col="annee_creation"):
    """Filtre un DataFrame de sociétés selon l'activité, l'effectif et la plage d'années."""
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[df[year_col].between(year_range[0], year_range[1])]
    return df

# Applique un prétraitement global une seule fois
df_societe_global = preprocess_societe(df_societe_global)
df_financements_global = preprocess_financements(df_financements_global)

###############################################
# 3) Importation des pages (multi-page layout)
###############################################
from dash_app.pages import home, projet, dashboard2, map, equipe, amelioration

# Barre de navigation (exemple)
navbar = dbc.NavbarSimple(
    brand="StartHub",
    brand_href="/",
    color="primary",
    dark=True,
    children=[
        dbc.NavItem(dbc.NavLink("Equipe", href="/equipe")),
        dbc.NavItem(dbc.NavLink("Projet", href="/projet")),
        dbc.NavItem(dbc.NavLink("Accueil", href="/home")),
        dbc.NavItem(dbc.NavLink("Dashboard", href="/dashboard2")),
        dbc.NavItem(dbc.NavLink("Carte", href="/map")),
        dbc.NavItem(dbc.NavLink("Amélioration", href="/amelioration"))
    ]
)

# Layout principal
app.layout = html.Div([
    navbar,
    dcc.Location(id="url", refresh=False),
    html.Div(id="page-content")
])

# Callback pour changer de page en fonction de l'URL
@app.callback(
    dash.dependencies.Output("page-content", "children"),
    [dash.dependencies.Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/dashboard2":
        return dashboard2.layout
    elif pathname == "/projet":
        return projet.layout
    elif pathname == "/equipe":
        return equipe.layout
    elif pathname == "/amelioration":
        return amelioration.layout
    elif pathname == "/map":
        return map.layout
    else:
        # Par défaut, on renvoie la page home
        return home.layout

if __name__ == "__main__":
    # Exemple : 1 seul worker pour éviter de saturer la RAM
    # (ou ajuster gunicorn dans votre Procfile)
    app.run_server(host="0.0.0.0", port=8080, debug=False)

