from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback

# Chargement des données
df = pd.read_csv('assets/societes.csv')

layout = dbc.Container([

    # Titre principal
    html.H1("Startups", className="text-center my-4"),

    # KPI Cards
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Start-ups", className="text-center"),
                html.H2(f"{df.nom.nunique()}", className="text-center")
            ])
        ]), width=4),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Domaines d'activités", className="text-center"),
                html.H2(f"{df.mots_cles_def.nunique()}", className="text-center")
            ])
        ]), width=4)
    ], justify="center", className="mb-4"),

    # Dropdown
    dcc.Dropdown(
        df.nom.unique(),
        id='df-dropdown',
        placeholder='Sélectionnez une start-up',
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
                html.P(f"Année de création: {startup_data['date_creation_def']}", className="text-center"),
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
