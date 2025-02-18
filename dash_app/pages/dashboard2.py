# dash_app/pages/dashboard2.py
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from collections import Counter
from dash_app.utils.data_loader import get_dataframe
from dash_app.app import app



# Import des fonctions utilitaires et de l'instance de l'app
from dash_app.utils.preprocessing import preprocess_societe, preprocess_financements, filter_societe

########################################
# Chargement et prétraitement des données
########################################
df_societe = get_dataframe('societes.csv')
df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year

if df_societe["annee_creation"].notna().sum() > 0:
    min_year = int(df_societe["annee_creation"].min())
    max_year = int(df_societe["annee_creation"].max())
else:
    min_year, max_year = 1986, 2025

df_financements = get_dataframe('financements.csv')
df_financements['Date dernier financement'] = pd.to_datetime(df_financements['Date dernier financement'], errors='coerce')
df_financements['Année'] = df_financements['Date dernier financement'].dt.year
df_financements['Montant_def'] = pd.to_numeric(df_financements["Montant_def"], errors='coerce').fillna(0)

########################################
# Layout de la page Dashboard
########################################
layout = html.Div([
    # En-tête
    html.Div([
        dbc.Container([ 
            dbc.Row([
                dbc.Col([
                    html.H1("Dashboard Financement", className="hero-title mb-4"),
                    html.H5("Analyse du financement de l'écosystème startup français", className="hero-subtitle mb-4")
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True) 
    ], className="hero-section mb-5"),

    dbc.Container([
        # Filtres
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("Activité principale", className="text-muted mb-2"),
                        dcc.Dropdown(
                            id="sector-filter",
                            options=[{"label": sec, "value": sec} for sec in sorted(df_societe["Activité principale"].dropna().unique())],
                            placeholder="Tous les secteurs",
                            multi=True,
                            className="mb-3"
                        )
                    ], md=4),
                    dbc.Col([
                        html.Label("Année de Création ou de financement", className="text-muted mb-2"),
                        dcc.RangeSlider(
                            id="year-filter",
                            min=1986,
                            max=max_year,
                            value=[min_year, max_year],
                            marks={i: str(i) for i in range(min_year, max_year + 1, 4)},
                            className="mb-3"
                        )
                    ], md=4),
                    dbc.Col([
                        html.Label("Taille d'effectif", className="text-muted mb-2"),
                        dcc.Dropdown(
                            id="effectif-filter",
                            options=[{"label": eff, "value": eff} for eff in sorted(df_societe["Effectif_def"].dropna().unique())],
                            placeholder="Toutes les tailles",
                            multi=True,
                            className="mb-3"
                        )
                    ], md=4)
                ])
            ])
        ], className="shadow-sm mb-4"),

        # KPI Cards
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="total-funding", className="metric-value")],
                                     className="metric-number"),
                            html.P("Financement Total", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="mean-funding", className="metric-value")],
                                     className="metric-number"),
                            html.P("Financement Moyen par entreprise", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="nbre-startup", className="metric-value")],
                                     className="metric-number"),
                            html.P("Startups créées", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="pourc-leve", className="metric-value")],
                                     className="metric-number"),
                            html.P("Entreprises ayant levé des fonds", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            )
        ]),

       # Graphiques
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Répartition des financements par typologie"),
                    dbc.CardBody([dcc.Graph(id="serie-funding")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Évolution des Financements"),
                    dbc.CardBody([dcc.Graph(id="funding-evolution")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Top 10 des entreprises ayant levé le plus de fonds"),
                    dbc.CardBody([dcc.Graph(id="top-funded")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Évolution de la création de startups par an"),
                    dbc.CardBody([dcc.Graph(id="startup-year")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Distribution des secteurs d'activités"),
                    dbc.CardBody([dcc.Graph(id="top-sector")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Distribution des startups par effectif (TOP 5)"),
                    dbc.CardBody([dcc.Graph(id="top-startup-size")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Top 10 des sous-catégories de mots-clés"),
                    dbc.CardBody([dcc.Graph(id="top-subcategories")])
                ], className="shadow-sm"), md=12, className="mb-4"
            )
        ]), 

        dbc.Container([...], fluid=True)  
    ])
])
########################################
# Callbacks de la page Dashboard
########################################

@app.callback(
    Output("mean-funding", "children"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def mean_funding_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    unique_count = df_fin["entreprise_id"].nunique()
    mean_fund = (df_fin["Montant_def"].sum() / unique_count) if unique_count > 0 else 0
    return f"{mean_fund:,.0f} €".replace(",", " ")

@app.callback(
    Output("total-funding", "children"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def total_funding_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    total = df_fin["Montant_def"].sum()
    return f"{total:,.0f} €".replace(",", " ")

@app.callback(
    Output("funding-evolution", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def update_funding_graph_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    funding_by_year = df_fin.groupby("Année")["Montant_def"].sum().reset_index()
    fig = px.line(funding_by_year, x="Année", y="Montant_def")
    return fig

@app.callback(
    Output("serie-funding", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def update_series_graph_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    funding_by_series = df_fin["Série"].value_counts().nlargest(10).reset_index()
    funding_by_series.columns = ["Série", "Count"]
    fig = px.bar(funding_by_series, x="index", y="Série")
    return fig

@app.callback(
    Output("startup-year", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def startup_per_year_cb(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    startups_by_year = df_soc.groupby("annee_creation")["entreprise_id"].count().reset_index()
    startups_by_year.columns = ["annee_creation", "nombre_startups"]
    fig = px.line(startups_by_year, x="annee_creation", y="nombre_startups")
    return fig

@app.callback(
    Output("top-funded", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def top_funded_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    top_funded_companies = df_fin.groupby("entreprise_id")["Montant_def"].sum().nlargest(10).reset_index()
    top_funded_companies = top_funded_companies.merge(df_soc, on="entreprise_id", how="left")
    fig = px.bar(top_funded_companies, x="nom", y="Montant_def")
    return fig

@app.callback(
    Output("pourc-leve", "children"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def pourc_levee_cb(sector, year_range, effectif):
    df_fin = preprocess_financements(get_dataframe("financements.csv"))
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    df_fin = df_fin[df_fin["entreprise_id"].isin(df_soc["entreprise_id"])]
    nb_total = df_soc.shape[0]
    nb_funded = df_fin["Montant_def"].nunique()
    pourc = (nb_funded / nb_total * 100) if nb_total > 0 else 0
    return f"{pourc:.2f}%"

@app.callback(
    Output("nbre-startup", "children"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def nbre_startup_cb(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    nbre = df_soc["nom"].nunique()
    return f"{nbre:,}".replace(",", " ")

@app.callback(
    Output("top-sector", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def top_sector_cb(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    if sector:
        df_soc = df_soc[df_soc["Activité principale"].isin(sector)]
    if effectif:
        df_soc = df_soc[df_soc["Effectif_def"].isin(effectif)]
    if year_range:
        df_soc = df_soc[df_soc["annee_creation"].between(year_range[0], year_range[1])]
    # Mapping des codes secteur aux noms
    dict_secteurs = {
        '01': 'Agriculture',
        '02': 'Sylviculture et exploitation forestière'
        # ... ajouter d'autres mappings si nécessaire
    }
    df_soc['Secteur'] = df_soc["Activité principale"].str[:2]
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
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def top_startup_size_cb(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    df_soc = filter_societe(df_soc, sector, effectif, year_range)
    distribution = df_soc["Effectif_def"].value_counts().reset_index()
    distribution.columns = ["Effectif", "Count"]
    distribution_top5 = distribution.sort_values(by="Count", ascending=True).tail(5)
    fig = px.pie(distribution_top5, names="Effectif", values="Count")
    return fig

@app.callback(
    Output("top-subcategories", "figure"),
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
def top_subcategories_cb(sector, year_range, effectif):
    df_soc = preprocess_societe(get_dataframe("societes.csv"))
    if sector:
        df_soc = df_soc[df_soc["Activité principale"].isin(sector)]
    if effectif:
        df_soc = df_soc[df_soc["Effectif_def"].isin(effectif)]
    if year_range:
        df_soc = df_soc[df_soc["annee_creation"].between(year_range[0], year_range[1])]
    df_soc["Sous-Catégorie"] = df_soc["Sous-Catégorie"].apply(lambda x: x.split("|") if isinstance(x, str) else [])
    df_exploded = df_soc.explode("Sous-Catégorie")
    filtered_df = df_exploded[df_exploded["Sous-Catégorie"] != "Divers"]
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
