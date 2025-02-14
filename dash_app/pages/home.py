from dash import Dash, html, dcc, Output, Input, State, ALL, callback, callback_context
import dash_bootstrap_components as dbc
import pandas as pd
from app import get_dataframe
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline

# Chargement des données
df = get_dataframe('societes.csv')

# Préparation des données pour KNN
keywords_dummies = df['mots_cles_def'].str.get_dummies(sep=', ')
market_dummies = df['market'].str.get_dummies(sep=', ')
activite_dummies = df['Activité principale'].str.get_dummies(sep=', ')

X_extended = pd.concat([keywords_dummies, market_dummies, activite_dummies], axis=1)
X_extended.reset_index(drop=True, inplace=True)

# Entraînement du modèle KNN
pipeline = Pipeline([
    ('knn', NearestNeighbors(n_neighbors=13, metric='manhattan'))
])
pipeline.fit(X_extended)

# Fonction de recommandation
def recommend_societes(selected_startup, data, X_extended, pipeline):
    if selected_startup not in data['nom'].values:
        return pd.DataFrame()

    entreprise_index = data.index[data['nom'] == selected_startup].tolist()[0]
    entreprise_data = X_extended.loc[entreprise_index].to_frame().T

    distances, indices = pipeline.named_steps['knn'].kneighbors(entreprise_data)
    voisins = data.iloc[indices[0]].copy()
    voisins['Distance'] = distances[0]
    voisins = voisins[voisins['nom'] != selected_startup]  # Exclure la société sélectionnée
    voisins = voisins.sort_values(by='Distance').head(10)  # Prendre les 10 plus proches

    return voisins[['nom', 'description', 'logo', 'mots_cles_def', 'market', 'Activité principale']]

# Stockage de la startup sélectionnée
layout = dbc.Container([
    dcc.Store(id="selected-startup", data=df["nom"].iloc[0]),

    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Découvrez les startups", className="hero-title mb-4"),
                    html.H5("Découvrez les 10,000 entreprises", className="hero-subtitle mb-4"),
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    # Sélecteur de startup
    dcc.Dropdown(
        id='df-dropdown',
        options=[{'label': name, 'value': name} for name in df.nom.unique()],
        value=df["nom"].iloc[0],
        placeholder='Sélectionnez ou entrez une start-up',
        searchable=True,
        className="mb-4"
    ),

    # Affichage des informations sur la startup sélectionnée
    html.Div(id="startup-info", className="text-center text-light"),
    html.Br(),

    html.H2("Ces sociétés peuvent vous intéresser :", className="section-title text-center mb-5"),
    html.Div(id="recommended-startups", className="mt-5"),
], fluid=True)


# Mise à jour de la startup sélectionnée via dropdown ou clic
@callback(
    Output("selected-startup", "data"),
    [Input("df-dropdown", "value"),
     Input({"type": "recommended-startup", "index": ALL}, "n_clicks")],
    [State({"type": "recommended-startup", "index": ALL}, "id")]
)
def update_selected_startup(selected_startup, n_clicks, button_ids):
    ctx = callback_context

    if not ctx.triggered:
        return selected_startup

    trigger_id = ctx.triggered[0]['prop_id']

    if "df-dropdown" in trigger_id:
        return selected_startup

    elif "recommended-startup" in trigger_id:
        for i, n in enumerate(n_clicks):
            if n and button_ids[i]:
                return button_ids[i]["index"]

    return selected_startup


# Mise à jour des informations de la startup sélectionnée
@callback(
    [Output("startup-info", "children"),
     Output("recommended-startups", "children")],
    [Input("selected-startup", "data")]
)
def update_startup_info(selected_startup):
    if not selected_startup:
        return "", ""

    startup_data = df[df["nom"] == selected_startup].iloc[0]

    # Ajout des catégories (bouton car c'est jolie... avoir par la suite si possible de faire cliquable)
    categories_buttons = [
        html.Button(
            category,
            className="btn btn-outline-primary btn-sm mx-1 disabled"
        ) for category in startup_data["Sous-Catégorie"].split("|") if category
    ]

    # Affichage des informations principales
    startup_card = dbc.Row([
        # Colonne Infos Startup
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Img(src=startup_data["logo"], style={"width": "200px", "margin": "0 auto", "display": "block"}),
                html.H3(startup_data["nom"], className="text-center mt-3"),
                html.P(f"Date de création: {startup_data['date_creation_def']}", className="text-center"),
                html.P(f"SIRET: {startup_data['SIRET']}", className="text-center"),
                html.P(f"Marché: {startup_data['market']}", className="text-center"),
                html.P(f"Effectifs: {startup_data['Effectif_def']}", className="text-center"),
                html.P(f"Type d'organisme: {startup_data['Type d\'organisme']}", className="text-center"),
                html.P(f"Catégorie: ", className="text-center"),
                html.Div(categories_buttons, className="d-flex justify-content-center")
            ])
        ]), width=4),

        # Colonne Description
        dbc.Col(dbc.Card([
            dbc.CardHeader("Description"),
            dbc.CardBody(html.P(startup_data["description"]))
        ]), width=4),

        # Colonne Contact
        dbc.Col(dbc.Card([
            dbc.CardHeader("Informations de contact"),
            dbc.CardBody([
                html.P([html.Strong("Adresse: "), startup_data["adresse_def"]]),
                html.P([html.Strong("Site web: "), html.A(startup_data["site_web_def"], href=startup_data["site_web_def"], target="_blank")])
            ])
        ]), width=4)
    ])

    # Affichage des recommandations
    recommended = recommend_societes(selected_startup, df, X_extended, pipeline)
    recommended_cards = [
        dbc.Col(
            dbc.Card([
                dbc.CardBody([
                    html.Button(
                        html.H5(row["nom"], className="text-center"),
                        id={"type": "recommended-startup", "index": row["nom"]},
                        n_clicks=0,
                        style={"border": "none", "background": "none"}
                    ),
                    html.Img(
                        src=row["logo"] if pd.notna(row["logo"]) else "/assets/default_logo.png",
                        style={"width": "300px", "height": "300px", "object-fit": "contain", "margin": "0 auto", "display": "block"}
                    ),
                    html.Br(
                    ),
                ])
            ], className="tech-card text-center shadow-sm"),
            width=3
        ) for _, row in recommended.iterrows()
    ]

    recommended_card = dbc.Row(recommended_cards, className="g-4 justify-content-center")

    return startup_card, recommended_card