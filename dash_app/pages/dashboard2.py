import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from app import get_dataframe  # Importer app et la fonction get_dataframe

# Chargement des données
df_societe = get_dataframe('societes.csv')

layout = html.Div([
    # Section Header
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Dashboard Financement", className="hero-title mb-4"),
                    html.H5("Analyse du financement de l'écosystème startup français", className="hero-subtitle mb-4")
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    dbc.Container([
        # Filtres
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(
                            id="sector-filter",
                            options=[{"label": secteur, "value": secteur} for secteur in df_societe["Activité principale"].dropna().unique()],
                            placeholder="Tous les secteurs",
                            multi=True,
                            className="mb-3"
                        ), 
                        html.Label("Année de Création", className="text-muted mb-2"),
                        dcc.RangeSlider(
                            id="year-filter",
                            min=2010,
                            max=2024,
                            value=[2010, 2024],
                            marks={i: str(i) for i in range(2010, 2025, 2)},
                            className="mb-3"
                        ),
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
                ], className="shadow-sm mb-4"),

        # KPI Cards
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
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  # ✅ Correction ici

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="total-funding", className="metric-value"),
                                html.Span("€", className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Montant total des financements", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  # ✅ Correction ici

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="avg-funding", className="metric-value"),
                                html.Span("€", className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Financement Moyen", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  # ✅ Correction ici

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="active-startups", className="metric-value"),
                                html.Span("%", className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Startups Actives", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4")  # ✅ Correction ici
        ]),

        # Graphiques
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Répartition par Secteur"),
                    dbc.CardBody([
                        dcc.Graph(id="sector-distribution")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Évolution des Financements"),
                    dbc.CardBody([
                        dcc.Graph(id="funding-evolution")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4")
        ]),
], fluid=True)
])