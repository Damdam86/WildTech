from dash import Dash, html, dcc, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.graph_objects as go
import plotly.express as px

# Chargement des données
df = get_dataframe('societes.csv')

# Moyenne des longitude et lat
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()


@callback(
    Output("image-container", "children"),
    Input("map-graph", "hoverData")
)
def display_hover_image(hoverData):
    if hoverData is None:
        return ""

    image_url = hoverData["points"][0]["customdata"][0]

    return html.Img(src=image_url, style={"width": "200px", "height": "200px"})

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
            hover_data={"adresse_def": True, "logo": False},
            custom_data=["logo"],
            zoom=5,
            center={"lat": center_lat, "lon": center_lon},
        )
        fig.update_traces(hovertemplate="<b>%{hovertext}</b><br>%{hoverdata[0]}")

    fig.update_layout(
        title="Carte des Startups",
        mapbox_style="open-street-map",
        margin={"r":0,"t":0,"l":0,"b":0}
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
                                html.Label("Recherche par ville ou code postal"),
                                dbc.Input(
                                    id="location-search",
                                    type="text",
                                    placeholder="Entrez une ville ou un code postal",
                                    className="mb-3"
                                ),
                            ], md=6),
                            dbc.Col([
                                html.Label("Recherche par mots-clés"),
                                dbc.Input(
                                    id="keyword-search",
                                    type="text",
                                    placeholder="Entrez des mots-clés",
                                    className="mb-3"
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
                        dcc.Graph(id='map-graph', figure=create_map(), style={"height": "600px"})
                    ])
                ])
            ], width=12)
        ], className="mb-5"),
    ])
])

# Callbacks
# State : Ne déclenche pas le callback mais sa valeur est accessible quand le callback est exécuté
@callback(
    Output('map-graph', 'figure'),
    [Input('search-button', 'n_clicks')],
    [State('location-search', 'value'),
    State('keyword-search', 'value')]
)
def update_map(n_clicks, location, keywords):
    if n_clicks is None:
        return create_map()

    filtered_df = df.copy()

    if location:
        location = location.lower()
        filtered_df = filtered_df[filtered_df['adresse_def'].str.lower().str.contains(location, na=False)]

    if keywords:
        keywords = keywords.lower()
        filtered_df = filtered_df[filtered_df['mot_cles_def'].str.lower().str.contains(keywords, na=False)]

    return create_map(filtered_df)
