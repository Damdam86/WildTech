from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe


# Chargement des données
df = get_dataframe('societes.csv')
df_financement = get_dataframe('financements.csv')
df_personne = get_dataframe('personnes.csv')

layout = dbc.Container([

    # Titre principal
    html.H1("Dashboard", className="text-center my-4"),

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

    # Section affichage des informations sur la start-up sélectionnée
    html.Div(id="startup-info", className="text-center text-light"),

], fluid=True)