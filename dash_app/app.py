import os
import json
import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask_caching import Cache
from wordcloud import WordCloud
from collections import Counter

# Initialisation de l'application
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
server = app.server  # Expose le serveur Flask pour Gunicorn
cache = Cache(app.server, config={'CACHE_TYPE': 'simple'})
TIMEOUT = None  # Cache permanent jusqu'au redémarrage de l'app

# --- Chargement des données ---
@cache.memoize(timeout=TIMEOUT)
def load_dataframes():
    data = {}
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
    files = ["societes.csv", "financements.csv", "personnes.csv"]
    for file in files:
        path = os.path.join(base_path, file)
        data[file] = pd.read_csv(path, low_memory=False)
    return data

def get_dataframe(filename):
    # Renvoie une copie du DataFrame pour éviter les modifications sur le cache
    data = load_dataframes()
    return data[filename].copy()

# --- Prétraitement commun ---
def preprocess_societe(df_societe):
    # Convertir la date et extraire l'année
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year
    return df_societe

def preprocess_financements(df):
    # Conversion des dates et des montants
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0)
    return df

def filter_societe(df, sector, effectif, year_range, year_col="annee_creation"):
    # Appliquer les filtres communs sur le DataFrame des sociétés
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[df[year_col].between(year_range[0], year_range[1])]
    return df

# --- Importation des pages ---
from .pages import home, projet, dashboard2, map, equipe, amelioration

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
    ]
)

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

# ----------------- CALLBACKS DE LA PAGE DASHBOARD2 -----------------

@app.callback(
    Output("mean-funding", "children"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def mean_funding(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    # Filtrer les sociétés
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    # Calcul du financement moyen par entreprise
    unique_count = df_fin["entreprise_id"].nunique()
    mean_fund = (df_fin['Montant_def'].sum() / unique_count) if unique_count > 0 else 0

    return f"{mean_fund:,.0f} €".replace(",", " ")

@app.callback(
    Output("total-funding", "children"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def total_funding(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    # Appliquer les filtres
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    total = df_fin['Montant_def'].sum()
    return f"{total:,.0f} €".replace(",", " ")

@app.callback(
    Output("funding-evolution", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def update_funding_graph(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    funding_by_year = df_fin.groupby('Année')['Montant_def'].sum().reset_index()
    fig = px.line(funding_by_year, x='Année', y='Montant_def')
    return fig

@app.callback(
    Output("serie-funding", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def update_series_graph(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    funding_by_series = df_fin["Série"].value_counts().nlargest(10).reset_index()
    funding_by_series.columns = ['Série', 'Count']
    fig = px.bar(funding_by_series, x='index', y='Série')
    return fig

@app.callback(
    Output("startup-year", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def startup_per_year(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    startups_by_year = df_soc.groupby('annee_creation').agg({'entreprise_id': 'count'}).reset_index()
    startups_by_year.columns = ['annee_creation', 'nombre_startups']
    fig = px.line(startups_by_year, x='annee_creation', y='nombre_startups')
    return fig

@app.callback(
    Output("top-funded", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def top_funded(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))

    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    top_funded_companies = (df_fin.groupby("entreprise_id")["Montant_def"]
                            .sum().nlargest(10).reset_index())
    top_funded_companies = top_funded_companies.merge(df_soc, on="entreprise_id", how="left")
    fig = px.bar(top_funded_companies, x='nom', y='Montant_def')
    return fig

@app.callback(
    Output("pourc-leve", "children"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def pourc_levee(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]

    nb_total = df_soc.shape[0]
    # Ici, le calcul peut être ajusté selon votre logique
    nb_funded = df_fin["Montant_def"].nunique()
    pourc = (nb_funded / nb_total * 100) if nb_total > 0 else 0
    return f"{pourc:.2f}%"

@app.callback(
    Output("nbre-startup", "children"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def nbre_startup(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    nbre = df_soc['nom'].nunique()
    return f"{nbre:,}".replace(",", " ")

@app.callback(
    Output("top-sector", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def top_sector(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    if sector:
        df_soc = df_soc[df_soc["Activité principale"].isin(sector)]
    if effectif:
        df_soc = df_soc[df_soc["Effectif_def"].isin(effectif)]
    if year_range:
        df_soc = df_soc[df_soc["annee_creation"].between(year_range[0], year_range[1])]
        
    # Mapper les codes secteur aux noms
    dict_secteurs = {
        '01': 'Agriculture',
        '02': 'Sylviculture et exploitation forestière',
        # … ajoutez les autres mappings
    }
    df_soc['Secteur'] = df_soc['Activité principale'].str[:2]
    df_soc['Nom Secteur'] = df_soc['Secteur'].map(dict_secteurs)
    distribution = df_soc['Nom Secteur'].value_counts().reset_index()
    distribution.columns = ['Nom Secteur', 'Count']
    distribution_top5 = distribution.sort_values(by='Count', ascending=True).tail(5)
    fig = px.bar(distribution_top5, x='Count', y='Nom Secteur',
                 labels={'Nom Secteur': 'Secteur', 'Count': 'Nombre'},
                 text='Count')
    return fig

@app.callback(
    Output("top-startup-size", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def top_startup_size(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    distribution = df_soc['Effectif_def'].value_counts().reset_index()
    distribution.columns = ['Effectif', 'Count']
    distribution_top5 = distribution.sort_values(by='Count', ascending=True).tail(5)
    fig = px.pie(distribution_top5, names='Effectif', values='Count')
    return fig

@app.callback(
    Output("cloud-words", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def cloud_words(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_soc['mots_cles_def'] = df_soc['mots_cles_def'].fillna('')
    all_keywords = ','.join(df_soc['mots_cles_def'].astype(str))
    keywords_list = [kw.strip() for kw in all_keywords.split(',') if kw.strip()]
    keywords_freq = Counter(keywords_list)
    wordcloud = WordCloud(width=1000, height=600, background_color='white', colormap='viridis').generate_from_frequencies(keywords_freq)
    img = wordcloud.to_array()
    fig = go.Figure(go.Image(z=img))
    fig.update_layout(margin={"r": 10, "t": 40, "l": 10, "b": 10},
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return fig

@app.callback(
    Output("top-subcategories", "figure"),
    [Input('sector-filter', 'value'),
     Input('year-filter', 'value'),
     Input('effectif-filter', 'value')]
)
def top_subcategories(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    if sector:
        df_soc = df_soc[df_soc["Activité principale"].isin(sector)]
    if effectif:
        df_soc = df_soc[df_soc["Effectif_def"].isin(effectif)]
    if year_range:
        df_soc = df_soc[df_soc["annee_creation"].between(year_range[0], year_range[1])]
    df_soc["Sous-Catégorie"] = df_soc["Sous-Catégorie"].apply(lambda x: x.split("|") if isinstance(x, str) else [])
    df_exploded = df_soc.explode("Sous-Catégorie")
    filtered_df = df_exploded[df_exploded['Sous-Catégorie'] != "Divers"]
    category_counts = filtered_df["Sous-Catégorie"].value_counts().reset_index()
    category_counts.columns = ["Sous-Catégorie", "Nombre de sociétés"]
    category_counts = category_counts.sort_values(by="Nombre de sociétés", ascending=True)
    fig = go.Figure(go.Bar(
        x=category_counts["Nombre de sociétés"],
        y=category_counts["Sous-Catégorie"],
        orientation='h', marker=dict(color='royalblue')
    ))
    fig.update_layout(xaxis_title="Nombre de Sociétés", yaxis_title="Sous-Catégorie",
                      template="plotly_white", height=700)
    return fig

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run_server(host="0.0.0.0", port=port, debug=False)
