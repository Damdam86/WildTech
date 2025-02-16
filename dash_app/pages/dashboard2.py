from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from app import get_dataframe  # Import absolu depuis le dossier racine
import plotly.express as px

################################################################################# CHARGEMENT DONNEES ##############################################################
df_societe = get_dataframe('societes.csv')
df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extrait uniquement l'année

if df_societe["annee_creation"].notna().sum() > 0:
    min_year = int(df_societe["annee_creation"].min())
    max_year = int(df_societe["annee_creation"].max())

df = get_dataframe('financements.csv')
df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
df['Année'] = df['Date dernier financement'].dt.year
df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0)


################################################################################ LAYOUT ###############################################################################

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
            # Filtre Activité principale
            dbc.Col([
                html.Label("Activité principale", className="text-muted mb-2"),
                dcc.Dropdown(
                    id="sector-filter",
                    options=[{"label": secteur, "value": secteur} for secteur in df_societe["Activité principale"].dropna().unique()],
                    placeholder="Tous les secteurs",
                    multi=True,
                    className="mb-3"
                )
            ], md=4),

            # Filtre Année de Création
            dbc.Col([
                html.Label("Année de Création ou de financement", className="text-muted mb-2"),
                dcc.RangeSlider(
                    id="year-filter",
                    min=1986,
                    max=max_year,
                    value=[min_year, max_year],
                    marks={i: str(i) for i in range(min_year, max_year + 1, 4)},
                    className="mb-3"
                )
            ], md=4),

            # Filtre Taille d'effectif
            dbc.Col([
                html.Label("Taille d'effectif", className="text-muted mb-2"),
                dcc.Dropdown(
                    id="effectif-filter",
                    options=[{"label": effectif, "value": effectif} for effectif in df_societe["Effectif_def"].dropna().unique()],
                    placeholder="Toutes les tailles",
                    multi=True,
                    className="mb-3"
                )
            ], md=4)
        ])
    ])
], className="shadow-sm mb-4"),

        # KPI Cards

        # Graph Financement total
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="total-funding", className="metric-value"),
                            ], className="metric-number"),
                            html.P("Financement Total", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  

        # Graph Financement moyen par société

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="mean-funding", className="metric-value"),
                                html.Span(className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Financement Moyen par entreprise", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  

        # Graph de nbre de création de sociétés

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="nbre-startup", className="metric-value"),
                                html.Span(className="metric-symbol")
                            ], className="metric-number"),
                            html.P("startups créées", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="pourc-leve", className="metric-value"),
                                html.Span(className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Des entreprises ont levé des fonds", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4")  
        ]),

        # Graphiques
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Répartition des financement par typologie"),
                    dbc.CardBody([
                        dcc.Graph(id="serie-funding")
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
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Top 10 des entreprises ayant levé le plus de fonds"),
                    dbc.CardBody([
                        dcc.Graph(id="top-funded")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Évolution création de startups par an"),
                    dbc.CardBody([
                        dcc.Graph(id="startup-year")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Distribution des secteurs d'activités"),
                    dbc.CardBody([
                        dcc.Graph(id="top-sector")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Distribution des StartUp par nombre de salariés (TOP 5)"),
                    dbc.CardBody([
                        dcc.Graph(id="top-startup-size")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Top 10 des sous-catégories de mots-clés"),
                    dbc.CardBody([
                        dcc.Graph(id="top-subcategories")
                    ])
                ], className="shadow-sm")
            ], md=12, className="mb-4"),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Nuage de mots clés"),
                    dbc.CardBody([
                        dcc.Graph(id="cloud-words")
                    ])
                ], className="shadow-sm")
            ], md=12, className="mb-4")

        ]),

], fluid=True)
])