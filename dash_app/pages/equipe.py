import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Données de l'équipe
team_members = [
    {"name": "Manon", "git": "https://github.com/manongte", "role": "La scrapeuse", "image": "./assets/Manon.png"},
    {"name": "Farid", "git": "https://github.com/Jouneyd", "role": "Le plotlyste", "image": "./assets/Farid.png"},
    {"name": "Nathalie", "git": "https://github.com/Nathlake", "role": "La SIREN", "image": "./assets/Nathalie.png"},
    {"name": "Damien", "git": "https://github.com/Damdam86", "role": "La pipelette", "image": "./assets/Damien.png"},
]

# Les cartes de l'équipe
team_cards = [
    dbc.Col(
        dbc.Card(
            [
                dbc.CardImg(src=member["image"], top=True, className="rounded-circle mx-auto d-block",
                            style={"width": "150px", "height": "150px"}),
                dbc.CardBody(
                    [
                        html.H5(member["name"], className="text-center"),
                        html.P(member["role"], className="text-center text-muted"),
                        html.A("GitHub", href=member["git"], target="_blank", className="metric-label text-center d-block mx-auto"),
                    ]
                ),
            ],
            className="metric-value",
            style={"border-radius": "10px", "overflow": "hidden"}
        ),
        md=3
    )
    for member in team_members
]

layout = html.Div([

    html.Div([ 
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Equipe", className="hero-title mb-4"),
                    html.H5("Meet the StartHub team", className="hero-subtitle mb-4")
                ], md=12)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    # Section des vignettes
    dbc.Container([
        dbc.Row(team_cards, className="justify-content-center"),
        html.Br(),
    ], fluid=True)
])
