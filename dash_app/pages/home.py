from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe
from sklearn.neighbors import NearestNeighbors
from sklearn.pipeline import Pipeline

# Chargement des données
df = get_dataframe('societes.csv')


#Recommandation

# Prépration des données pour KNN
keywords_dummies = df['mots_cles_def'].str.get_dummies(sep=', ')
market_dummies = df['market'].str.get_dummies(sep=', ')
activite_dummies = df['Activité principale'].str.get_dummies(sep=', ')

X_extended = pd.concat([keywords_dummies, market_dummies, activite_dummies], axis=1)
X_extended.reset_index(drop=True, inplace=True)

# Entrainement du modèle 
pipeline = Pipeline([
    ('knn', NearestNeighbors(n_neighbors=10, metric='manhattan'))
])
pipeline.fit(X_extended)

# Fonction de recommandation
def recommend_societes(selected_startup, data, X_extended, pipeline):
    """ Recommande 10 startups similaires à partir des mots-clés et activités. """
    if selected_startup not in data['nom'].values:
        return []

    entreprise_index = data.index[data['nom'] == selected_startup].tolist()[0]
    entreprise_data = X_extended.loc[entreprise_index].to_frame().T

    distances, indices = pipeline.named_steps['knn'].kneighbors(entreprise_data)
    voisins = data.iloc[indices[0]].copy()
    voisins['Distance'] = distances[0]
    voisins = voisins[voisins['nom'] != selected_startup]  # Exclure la société sélectionnée
    voisins = voisins.sort_values(by='Distance').head(10)  # Prendre les 10 plus proches

    return voisins[['nom', 'description', 'logo','mots_cles_def', 'market', 'Activité principale', 'Distance']]


layout = dbc.Container([

    # Hero Section avec image de fond et overlay
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Découvrez les startups", className="hero-title mb-4"),
                    html.H5("Découvrez les 10,000 entreprises", className="hero-subtitle mb-4"),], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

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
        options=[{'label': name, 'value': name} for name in df.nom.unique()],
        placeholder='Sélectionnez ou entrez une start-up',
        searchable=True,
        className="mb-4"
    ),

    # Section affichage des informations sur la start-up sélectionnée
    html.Div(id="startup-info", className="text-center text-light"),
    html.H2("Ces sociétés peuvent vous interesser :", className="section-title text-center mb-5"),    
    html.Div(id="recommended-startups", className="mt-5"),

], fluid=True)

# Callback pour mettre à jour les informations de la startup sélectionnée
@callback(
    [Output("startup-info", "children"),
     Output("recommended-startups", "children")],
    [Input("df-dropdown", "value")]
)
def update_startup_info(selected_startup):
    if not selected_startup:
        return "", ""

    startup_data = df[df["nom"] == selected_startup].iloc[0]

    # 📌 **Affichage des détails de la startup sélectionnée**
    startup_card = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Img(src=startup_data["logo"], style={"width": "150px", "margin": "0 auto", "display": "block"}),
                html.H3(startup_data["nom"], className="text-center mt-3"),
                html.P(f"Date de création: {startup_data['date_creation_def']}", className="text-center"),
                html.P(f"Effectifs: {startup_data['Effectif_def']}", className="text-center"),
                html.P(f"Type d'organisme: {startup_data["Type d'organisme"]}, {startup_data["market"]}", className="text-center"),
                html.P(startup_data['mots_cles_def'], className="text-center")
            ])
        ], className="tech-card"), width=4),

        dbc.Col(dbc.Card([
            dbc.CardHeader("Description"),
            dbc.CardBody(html.P(startup_data["description"]))
        ], className="tech-card"), width=4),

        dbc.Col(dbc.Card([
            dbc.CardHeader("Informations de contact"),
            dbc.CardBody([
                html.P([html.Strong("Adresse: "), startup_data["adresse_def"]]),
                html.P([html.Strong("Site web: "), html.A(startup_data["site_web_def"], href=startup_data["site_web_def"], target="_blank")])
            ])
        ], className="tech-card"), width=4)
    ], className="g-4")

    # 📌 **Recommandations**
    recommended = recommend_societes(selected_startup, df, X_extended, pipeline)
    recommended_cards = []
    for _, row in recommended.iterrows():
        recommended_cards.append(
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H5(row["nom"], className="text-center"),
                    html.Img(src=row["logo"], style={"width": "150px", "margin": "0 auto", "display": "block"}),
                    html.P(f"Mots clés: {row['mots_cles_def']}", className="text-center"),
                    html.P(f"Description: {row['description']}", className="text-left")
                ])
            ], className="tech-card"), width=4)
        )   

    recommended_card = dbc.Row(recommended_cards, className="g-4")

    return startup_card, recommended_card