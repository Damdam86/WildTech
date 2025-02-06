import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from app import get_dataframe  # Importer app et la fonction get_dataframe

# Chargement des données
df = get_dataframe('societes.csv')
df_financement = get_dataframe('financements.csv')
df_personne = get_dataframe('personnes.csv')

layout = html.Div([
    # Hero Section avec image de fond et overlay
    html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Dashboard Financement", className="hero-title mb-4"),
                html.H5(
                    "Analyse du financement de l'écosystème startup français",
                    className="hero-subtitle mb-4"
                ),
            ], md=8, lg=6)
        ], className="min-vh-75 align-items-center")
    ], fluid=True)
], className="hero-section mb-5"),
    dbc.Container([
        # Filtres
        html.Div([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Secteur d'Activité", className="text-muted mb-2"),
                            dcc.Dropdown(
                                id="sector-filter",
                                placeholder="Tous les secteurs",
                                multi=True,
                                className="mb-3"
                            )
                        ], md=4),
                        dbc.Col([
                            html.Label("Année de Création", className="text-muted mb-2"),
                            dcc.RangeSlider(
                                id="year-filter",
                                min=2010,
                                max=2024,
                                value=[2010, 2024],
                                marks={i: str(i) for i in range(2010, 2025, 2)},
                                className="mb-3"
                            )
                        ], md=4),
                        dbc.Col([
                            html.Label("Taille de Financement", className="text-muted mb-2"),
                            dcc.Dropdown(
                                id="funding-filter",
                                options=[
                                    {"label": "< 1M€", "value": "seed"},
                                    {"label": "1M€ - 5M€", "value": "series_a"},
                                    {"label": "5M€ - 20M€", "value": "series_b"},
                                    {"label": "> 20M€", "value": "growth"}
                                ],
                                placeholder="Toutes les tailles",
                                className="mb-3"
                            )
                        ], md=4)
                    ])
                ])
            ], className="shadow-sm mb-4")
        ]),
        
        # KPI Cards
        html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                html.Div([
                                    html.Span(id="total-startups", className="metric-value"),  
                                    html.Span("€", className="metric-symbol")
                                ], className="metric-number"),
                                html.P("Financement Total", className="metric-label")
                            ], className="metric-card")
                        ])

                ], width=12, md=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span(id="total-funding", className="metric-value"),
                            html.Span("€", className="metric-symbol")
                        ], className="metric-number"),
                        html.P("Financement Total", className="metric-label")
                    ], className="metric-card")
                ], width=12, md=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span(id="avg-funding", className="metric-value"),
                            html.Span("€", className="metric-symbol")
                        ], className="metric-number"),
                        html.P("Financement Moyen", className="metric-label")
                    ], className="metric-card")
                ], width=12, md=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span(id="active-startups", className="metric-value"),
                            html.Span("%", className="metric-symbol")
                        ], className="metric-number"),
                        html.P("Startups Actives", className="metric-label")
                    ], className="metric-card")
                ], width=12, md=3, className="mb-4")
            ])
        ], className="mb-5"),
        
        # Graphiques
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H4("Répartition par Secteur", className="mb-3"),
                        dcc.Graph(id="sector-distribution")
                    ], className="tech-card")
                ], md=6, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.H4("Évolution des Financements", className="mb-3"),
                        dcc.Graph(id="funding-evolution")
                    ], className="tech-card")
                ], md=6, className="mb-4")
            ]),
        ], className="mb-5")
    ], fluid=True, className="bg-white py-5")
], className="dashboard-page")
])