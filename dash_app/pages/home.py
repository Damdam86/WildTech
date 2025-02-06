from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe

# Chargement des données
df = get_dataframe('societes.csv')

layout = dbc.Container([

     # Section Hero
    dbc.Row([
        dbc.Col(html.H1(" ", className="hero-section"), width=12)
    ], className="mb-4"),

     # Section d'introduction
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H3("Découvrez les Startups Innovantes", className="text-center"),
                html.P("Explorez notre projet et utilisez notre tableau de bord interactif pour filtrer et analyser les données.", className="text-center"),
            ])
        ], className="shadow-sm"), width=8)
    ], justify="center", className="mb-4"),

    # Section Liens vers les autres pages
    dbc.Row([
        dbc.Col(dbc.Button("Explorer le Projet", href="/projet", color="secondary", className="w-100"), width=3),
        dbc.Col(dbc.Button("Accéder au Dashboard", href="/dashboard2", color="primary", className="w-100"), width=3),
    ], justify="center", className="mb-4"),

    # KPI Cards
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Start-ups", className="metric-label"),
                html.H2(f"{df.nom.nunique()}", className="metric-value")
            ])
        ]), width=4),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Domaines d'activités", className="metric-label"),
                html.H2(f"{df.mots_cles_def.nunique()}", className="metric-value")
            ])
        ]), width=4)
    ], justify="center", className="mb-4"),

    # Dropdown
    dcc.Dropdown(
        id='df-dropdown',
        options=[{'label': name, 'value': name} for name in df.nom.unique()[:3500]], #Test réduit sur 3500 pour pouvoir isoler le problème et lenteur A ENLEVER après que le df soit propre 
        placeholder='Sélectionnez ou entrez une start-up',
        searchable=True,
        className="mb-4"
    ),

    # Section affichage des informations sur la start-up sélectionnée
    html.Div(id="startup-info", className="text-center text-light"),

], fluid=True)

# Callback pour mettre à jour les informations de la startup sélectionnée
@callback(
    Output("startup-info", "children"),
    Input("df-dropdown", "value")
)
def update_startup_info(selected_startup):
    if not selected_startup:
        return ""

    startup_data = df[df["nom"] == selected_startup].iloc[0]

    return dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Img(src=startup_data["logo"], style={"width": "150px", "margin": "0 auto", "display": "block"}),
                html.H3(startup_data["nom"], className="text-center mt-3"),
                html.P(f"Date de création: {startup_data['date_creation_def']}", className="text-center"),
                html.P(f"Effectifs: {startup_data['Effectif_def']}", className="text-center"),
                html.P(f"Type d'organisme: {startup_data["Type d'organisme"]}, {startup_data["market"]}", className="text-center"),
                html.P(startup_data['mots_cles_def'], className="text-center")
            ])
        ], className="h-100"), width=4),

        dbc.Col(dbc.Card([
            dbc.CardHeader("Description"),
            dbc.CardBody(html.P(startup_data["description"]))
        ], className="h-100"), width=4),

        dbc.Col(dbc.Card([
            dbc.CardHeader("Informations de contact"),
            dbc.CardBody([
                html.P([html.Strong("Adresse: "), startup_data["adresse_def"]]),
                html.P([html.Strong("Site web: "), html.A(startup_data["site_web_def"], href=startup_data["site_web_def"], target="_blank")])
            ])
        ], className="h-100"), width=4)
    ], className="g-4")
