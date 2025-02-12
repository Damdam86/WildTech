import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px  # Importer Plotly pour les graphiques (si besoin)

# Layout de la page avec le bandeau d'introduction et les perspectives d'amélioration
layout = html.Div([

    # Section d'introduction avec bandeau
    html.Div([ 
        dbc.Container([
            dbc.Row([ 
                dbc.Col([ 
                    html.H1("Amélioration", className="hero-title mb-4"),
                    html.H5("Analyse du financement de l'écosystème startup français", className="hero-subtitle mb-4")
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    # Section des perspectives d'amélioration avec cards alignées sur la même ligne et centrées
    html.Div([
        dbc.Container([ 
            dbc.Row(
                justify="center",  # Centre les cartes horizontalement
                align="center",    # Centre les cartes verticalement dans le Row
                children=[
                    dbc.Col([

                        # Première carte pour "Actualisation en temps réel"
                        dbc.Card(
                            dbc.CardBody([
                                html.H4("1. Une actualisation automatique des données", className="card-title", style={'font-size': '22px', 'color': 'blue'}),
                                html.P(
                                    "sur les levées de fonds, les nouvelles startups et la performance financière. ",
                                    className="card-text", style={'font-size': '20px'}
                                ),
                            ]),
                            color="light", outline=True, className="mb-3", style={"max-width": "500px"}
                        ),

                    ], md=6, lg=6),  

                    dbc.Col([

                        dbc.Card(
                            dbc.CardBody([
                                html.H4("2. Un suivi des performances au fil de l'eau", className="card-title", style={'font-size': '22px', 'color': 'blue'}),
                                html.P(
                                    "comme l'évolution des revenus, "
                                    "des levées de fonds, ou de la taille de l’équipe.",
                                    className="card-text", style={'font-size': '20px'}
                                ),
                            ]),
                            color="light", outline=True, className="mb-3", style={"max-width": "500px"}
                        ),

                    ], md=6, lg=6) 
                ]
            ),


            dbc.Row(
                justify="center",  
                children=[
                    dbc.Col([
                        html.P(
                            "Ces améliorations permettraient d'obtenir des informations actualisées sur les tendances du marché.",
                            className="section-description", style={'font-size': '18px', 'text-align': 'center'}
                        ),
                    ], md=12)
                ]
            )
        ], fluid=True)
    ], className="improvements-section mb-5"),

])
