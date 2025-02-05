import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from flask_caching import Cache
from io import StringIO

# Initialisation de l'application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG], suppress_callback_exceptions=True)

#Configuration du cache
cache = Cache(app.server, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': 'cache-directory'})
TIMEOUT = None #cache permanent jusqu'à redémarrage de l'app 

#Chargement des csv
@cache.memoize(timeout=TIMEOUT)
def query_all_data():
    files = ["societes.csv", "financements.csv", "personnes.csv"]  # Liste des fichiers à charger
    dataframes = {}

    for file in files:
        df = pd.read_csv(f'assets/{file}')
        dataframes[file] = df.to_json(date_format='iso', orient='split')  # Stockage JSON

    return dataframes  # Retourne un dictionnaire JSON

def get_dataframe(filename):
    dataframes = query_all_data()  # Récupère tous les datasets en cache
    json_string = dataframes[filename]  # Récupère la chaîne JSON
    return pd.read_json(StringIO(json_string), orient='split')  # Convertit en DataFrame en utilisant StringIO pour le FutureWarning

from pages import home, dashboard, projet  # Importer les pages

# Barre de navigation
navbar = dbc.NavbarSimple(
    brand="StartHub",
    brand_href="/",
    color="primary",
    dark=True,
    children=[
        dbc.NavItem(dbc.NavLink("Accueil", href="/home")),
        dbc.NavItem(dbc.NavLink("Dashboard", href="/dashboard")),
        dbc.NavItem(dbc.NavLink("Projet", href="/projet"))
    ])

# Layout
app.layout = html.Div([
    navbar,
    dcc.Location(id='url', refresh=False),  # Location pour suivre l'URL
    html.Div(id='page-content')  # Contenu de la page à changer en fonction de l'URL
])

# Callback pour changer de page en fonction de l'URL
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/dashboard':
        return dashboard.layout
    elif pathname == '/projet':
        return projet.layout
    else:
        return home.layout

if __name__ == '__main__':
    app.run_server(debug=True)