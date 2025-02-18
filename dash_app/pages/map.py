from dash import html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
from ..app import get_dataframe
import plotly.graph_objects as go
import plotly.express as px
import ast

# Chargement et prétraitement des données
df = get_dataframe('societes.csv')
df['Sous-Catégorie'] = df['Sous-Catégorie'].apply(lambda x: x.split("|") if isinstance(x, str) else [])
unique_categories = sorted({cat for sublist in df['Sous-Catégorie'].dropna() for cat in sublist})
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()

@callback(
    [Output("image-container", "children"),
     Output("startup-name", "children"),
     Output("startup-date", "children"),
     Output("startup-category", "children")],
    Input("map-graph", "hoverData")
)
def display_hover_image(hoverData):
    if hoverData is None:
        return "", "", "", ""
    point = hoverData["points"][0]
    custom_data = point.get("customdata", [])
    while len(custom_data) < 5:
        custom_data.append("")
    name = point["hovertext"]
    image_url = custom_data[0] if custom_data[0] else None
    adresse = custom_data[1] if custom_data[1] else "Adresse non disponible"
    date_creation = custom_data[2] if custom_data[2] else "Non disponible"
    categories = custom_data[3] if custom_data[3] else "Non spécifiée"
    description = custom_data[4] if custom_data[4] else "Description non disponible"
    categories_list = ast.literal_eval(categories) if isinstance(categories, str) else []
    categories_buttons = [html.Button(category.strip(), className="btn btn-outline-primary btn-sm m-1 disabled")
                          for category in categories_list if category.strip()]
    startup_info = [f"Adresse: {adresse}", html.Br(), f"Date de création: {date_creation}",
                    html.Br(), f"Description: {description}", html.Br()]
    return (html.Img(src=image_url, style={"width": "150px", "margin": "0 auto", "display": "block"}),
            html.H3(name, className="text-center mt-3"),
            startup_info,
            html.Div(categories_buttons, className="d-flex justify-content-center flex-wrap"))

def create_map(filtered_df=None):
    if filtered_df is None:
        filtered_df = df
    fig = px.scatter_mapbox(
        filtered_df,
        lat="latitude",
        lon="longitude",
        hover_name="nom",
        hover_data={"logo": False, "adresse_def": True, "date_creation_def": False,
                    "Sous-Catégorie": False, "description": False,
                    "latitude": False, "longitude": False},
        zoom=5,
        center={"lat": center_lat, "lon": center_lon}
    )
    fig.update_traces(marker=dict(size=14),
                      cluster=dict(enabled=True, color="blue", opacity=0.7),
                      customdata=filtered_df[["logo", "adresse_def", "date_creation_def", "Sous-Catégorie", "description"]].astype(str).values)
    fig.update_layout(
        title="Carte des Startups",
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        dragmode="zoom",
        mapbox=dict(zoom=5, center={"lat": center_lat, "lon": center_lon})
    )
    return fig

layout = html.Div([
    # Hero Section
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("La carte", className="hero-title mb-4"),
                    html.H5("Retrouvez les startups proches de chez vous.", className="hero-subtitle mb-4")
                ], md=8, lg=8)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    # Section Recherche
    dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Filtres de recherche"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Recherche par ville"),
                                dbc.Input(id="location-search", type="text", placeholder="Entrez une ville", className="mb-3")
                            ], md=6),
                            dbc.Col([
                                html.Label("Recherche par catégorie"),
                                dcc.Dropdown(
                                    id='keyword-dropdown',
                                    options=[{'label': cat, 'value': cat} for cat in unique_categories],
                                    multi=True,
                                    placeholder="Sélectionnez une catégorie"
                                )
                            ], md=6),
                        ]),
                        dbc.Button("Rechercher", id="search-button", color="primary", className="mt-3")
                    ])
                ], className="mb-4")
            ], width=12)
        ])
    ]),

    # Section Carte
    dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Carte des Startups"),
                    dbc.CardBody([dcc.Graph(id='map-graph', figure=create_map(), style={"height": "600px"}, config={'scrollZoom': True})])
                ])
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Info startup"),
                    dbc.CardBody([
                        html.Div(id="image-container", style={"textAlign": "center"}),
                        html.Div(id="startup-name", className="text-center mt-3"),
                        html.Div(id="startup-date", className="text-center"),
                        html.Div(id="startup-category", className="text-center")
                    ])
                ])
            ], width=4)
        ], className="mb-5"),
    ])
])
