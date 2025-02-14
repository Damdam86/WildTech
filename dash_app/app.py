import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from flask_caching import Cache
import plotly.express as px
import os
from io import StringIO

# Initialisation de l'application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Configuration du cache (en mémoire pour économiser la mémoire)
cache = Cache(app.server, config={'CACHE_TYPE': 'simple'})
TIMEOUT = None  # Cache permanent jusqu'au redémarrage de l'app

# Chargement sécurisé des CSV
@cache.memoize(timeout=TIMEOUT)
def query_all_data():
    files = ["assets/societes.csv", "assets/financements.csv", "assets/personnes.csv"]
    dataframes = {}

    for file in files:
        if os.path.exists(file):  # Vérifie si le fichier existe
            try:
                df = pd.read_csv(file)
                dataframes[os.path.basename(file)] = df.to_json(date_format='iso', orient='split')
            except Exception as e:
                print(f"⚠️ Erreur lors du chargement de {file}: {e}")
        else:
            print(f"⚠️ Fichier introuvable : {file}")

    return dataframes

def get_dataframe(filename):
    dataframes = query_all_data()
    json_string = dataframes.get(filename, "{}")  # Sécuriser l'accès

    try:
        return pd.read_json(StringIO(json_string), orient='split')  # Utilisation de StringIO pour éviter FutureWarning
    except ValueError as e:
        print(f"⚠️ Erreur lors de la conversion JSON → DataFrame : {e}")
        return pd.DataFrame()  # Renvoie un DataFrame vide en cas d'erreur

# Importation des pages après les fonctions (évite l'import circulaire)
from pages import home, projet, dashboard2, map, equipe, amelioration

# Barre de navigation
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
    ])

# Layout principal
app.layout = html.Div([
    navbar,
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Callback pour changer de page en fonction de l'URL
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/dashboard2':
        return dashboard2.layout
    elif pathname == '/projet':
        return projet.layout
    elif pathname == '/equipe':
        return equipe.layout
    elif pathname == '/amelioration':
        return amelioration.layout
    elif pathname == '/map':
        return map.layout
    else:
        return home.layout

# Exemple de callback (corrigé)
@app.callback(
    Output("mean-funding", "children"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ]
)
def mean_funding(sector, year_range, effectif):
    df = get_dataframe("financements.csv")
    df['Montant_def'] = pd.to_numeric(df['Montant_def'], errors='coerce')

    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[(df_societe["annee_creation"] >= year_range[0]) &
                                (df_societe["annee_creation"] <= year_range[1])]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]

    mean_funding = df['Montant_def'].fillna(0).mean() if not df.empty else 0
    return f"{mean_funding:,.0f} €".replace(",", " ")

# Graphique Funding Evolution
@app.callback(
    Output("funding-evolution", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ]
)
def update_funding_graph(sector, year_range, effectif):
    df = get_dataframe("financements.csv")
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0)

    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year

    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[df_societe["annee_creation"].between(year_range[0], year_range[1])]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]

    funding_by_year = df.groupby('Année')['Montant_def'].sum().reset_index()
    fig = px.line(funding_by_year, x='Année', y='Montant_def')

    return fig

# Lancement de l'application avec gestion du port Render
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))  # Essaye un autre port
    print(f"🚀 Lancement de l'application sur le port {port}...")
    app.run_server(host="0.0.0.0", port=port, debug=True)