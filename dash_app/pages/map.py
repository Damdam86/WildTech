from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.graph_objects as go
import plotly.express as px


# Chargement des données
df = get_dataframe('societes.csv')

# Création de la fig carte
fig = px.scatter_mapbox(
    pd.DataFrame({'lat': [], 'lon': []}),
    lat="lat",
    lon="lon",
    zoom=5,
    center={"lat": 48.8566, "lon": 2.3522},  # Paris
)

fig.update_layout(
    title="Carte des Startups",
    mapbox_style="open-street-map",  
    margin={"r":0,"t":0,"l":0,"b":0}
)


################################################################################ LAYOUT ################################################################################


layout = html.Div([
# Hero Section avec image de fond et overlay
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("La carte", className="hero-title mb-4"),
                    html.H5(
                        "Retrouvez les startups proches de chez vous.",
                        className="hero-subtitle mb-4"
                    ),
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

# Section carte
            dbc.Row([
                dbc.Col([
                     dbc.Card([
                dbc.CardHeader("Carte des Startups"),
                dbc.CardBody([
                    dcc.Graph(figure=fig) 
                    ])
                ])
                ], width=12)
            ], className="mb-5"),

])
