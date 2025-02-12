import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Layout de la page avec bandeau d'introduction et perspectives d'amélioration
layout = html.Div([


    # Hero Section avec image de fond et overlay
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

    # Section des perspectives d'amélioration
        dbc.Container([
            html.Div([
                html.H2("Perspectives d'amélioration", className="section-title text-center mb-5"),
                dbc.Row([
                    dbc.Col([
                        html.Div([
                            html.Div([
                                html.Span("1", className="step-number"),
                                html.H4("Une actualisation automatique des données", className="metric-value"),
                                html.P("Sur les levées de fonds, les nouvelles startups et la performance financière.", className="text-muted"),
                            ], className="step-card-content")
                        ], className="step-card")
                    ], md=6, lg=3, className="mb-4"),

                    dbc.Col([
                        html.Div([
                            html.Div([
                                html.Span("2", className="step-number"),
                                html.H4("Un suivi des performances au fil du temps", className="metric-value"),
                                html.P("Comme l'évolution des revenus, des levées de fonds, ou de la taille de l’équipe.", className="text-muted"),
                            ], className="step-card-content")
                        ], className="step-card")
                    ], md=6, lg=3, className="mb-4")
                ]),

                dbc.Row([
                    dbc.Col([
                        html.P(
                            "Ces améliorations permettraient d'obtenir des informations actualisées sur les tendances du marché.",
                            className="section-description text-center", style={'font-size': '18px'}
                        ),
                    ], md=12)
                ])
            ], className="improvements-section mb-5")
        ], fluid=True, className="project-page")
    ])
