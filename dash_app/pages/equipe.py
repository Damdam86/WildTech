import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.express as px


layout = html.Div([
    
    html.Div([ 
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Equipe", className="hero-title mb-4"),
                html.H5("Analyse du financement de l'écosystème startup français", className="hero-subtitle mb-4")
            ], md=8, lg=6)
        ], className="min-vh-75 align-items-center")
    ], fluid=True)
], className="hero-section mb-5")
])