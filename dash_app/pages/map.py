from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.graph_objects as go
import plotly.express as px


# Chargement des données
df = get_dataframe('societes_geolocalisees.csv')

#Moyenne des longitude et lat
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()

# Création de la fig carte
if 'latitude' in df.columns and 'longitude' in df.columns:
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="nom",
        hover_data=["adresse_def"],
        zoom=5,
        center={"lat": center_lat, "lon": center_lon},)

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
                ], md=8, lg=8)
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
