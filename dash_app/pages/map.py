from dash import Dash, html, dcc, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.graph_objects as go
import plotly.express as px

# Chargement des données
df = get_dataframe('societes.csv')
df['Sous-Catégorie'] = df['Sous-Catégorie'].str.split("|")
df = df.explode('Sous-Catégorie')

# Moyenne des longitude et lat
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
    custom_data = point["customdata"] if "customdata" in point else ["", "", "", ""]
    name = point["hovertext"]
    image_url = custom_data[0] if custom_data[0] else None
    adresse = custom_data[1] if custom_data[1] else "Adresse non disponible"
    date_creation = custom_data[2] if custom_data[2] else "Non disponible"
    categories = custom_data[3] if custom_data[3] else "Non spécifiée"

     # Utilisation de html.Div et <br> pour le saut de ligne
    startup_info = [f"Adresse: {adresse}",
        html.Br(),
        f"Date de création: {date_creation}"]

    return (
        html.Img(src=image_url, style={"width": "150px", "margin": "0 auto", "display": "block"}),
        html.H3(name, className="text-center mt-3"),
        startup_info,
        f"Catégorie: {categories}"
    )

# Fonction pour créer la carte
def create_map(filtered_df=None):
    if filtered_df is None:
        filtered_df = df

    if 'latitude' in filtered_df.columns and 'longitude' in filtered_df.columns:
        fig = px.scatter_mapbox(
            filtered_df,
            lat="latitude",
            lon="longitude",
            hover_name="nom",
            hover_data={
                "logo": False,
                "adresse_def": True,
                "date_creation_def": False,
                "Sous-Catégorie": True,
                "latitude": False,
                "longitude": False
            },
            zoom=5,
            center={"lat": center_lat, "lon": center_lon},
        )

    fig.update_traces(marker=dict(size=14), cluster=dict(enabled=True, color="blue", opacity=0.7), # Affichage des clusters
                    customdata=filtered_df[["logo", "adresse_def", "date_creation_def", "Sous-Catégorie"]].values) 

    fig.update_layout(
    title="Carte des Startups",
    mapbox_style="open-street-map",
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    dragmode="zoom",  # Permet d'utiliser la molette pour zoomer
    mapbox=dict(
        zoom=5,
        center={"lat": center_lat, "lon": center_lon},
    )
)
    return fig


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

        # Section recherche
    dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Filtres de recherche"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Recherche par ville"),
                                dbc.Input(
                                    id="location-search",
                                    type="text",
                                    placeholder="Entrez une ville",
                                    className="mb-3"
                                ),
                            ], md=6),
                        
                            dbc.Col([
                                html.Label("Recherche par catégorie"),
                                dcc.Dropdown(
                                    id='keyword-dropdown',
                                    options=[{'label': cat, 'value': cat} for cat in df['Sous-Catégorie'].dropna().unique()],
                                    multi=True,
                                    placeholder="Sélectionnez une catégorie"
                                ),
                            ], md=6),
                        ]),
                        dbc.Button("Rechercher", id="search-button", color="primary", className="mt-3"),
                    ])
                ], className="mb-4")
            ], width=12)
        ])
    ]),

    # Section carte
    dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Carte des Startups"),
                    dbc.CardBody([
                        dcc.Graph(id='map-graph', figure=create_map(), style={"height": "600px"}, config={'scrollZoom': True})
                    ])
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

# Callbacks
# State : Ne déclenche pas le callback mais sa valeur est accessible quand le callback est exécuté
@callback(
    Output('map-graph', 'figure'),
    [Input('search-button', 'n_clicks')],
    [State('location-search', 'value'),
    State('keyword-dropdown', 'value')]
)
def update_map(n_clicks, location, selected_keywords):
    if n_clicks is None:
        return create_map()

    filtered_df = df.copy()

    if location:
        location = location.lower()
        filtered_df = filtered_df[filtered_df['adresse_def'].str.lower().str.contains(location, na=False)]

    if selected_keywords:
        filtered_df = filtered_df[filtered_df['Sous-Catégorie'].fillna("").apply(lambda x: any(kw in x for kw in selected_keywords))]

    return create_map(filtered_df)



