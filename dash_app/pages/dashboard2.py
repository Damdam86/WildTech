import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from app import get_dataframe  # Importer app et la fonction get_dataframe
import plotly.express as px

# Chargement des données
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

#Graphs
funding_by_year = df.groupby('Année')['Montant_def'].sum().reset_index()
fig1 = px.line(funding_by_year, x='Année', y='Montant_def')

#Layout
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
            # Colonne 1 : Activité principale
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

            # Colonne 2 : Année de Création
            dbc.Col([
                html.Label("Année de Création", className="text-muted mb-2"),
                dcc.RangeSlider(
                    id="year-filter",
                    min=2000,  
                    max=max_year,  
                    value=[min_year, max_year],  
                    marks={i: str(i) for i in range(min_year, max_year + 1, 2)}, 
                    className="mb-3"
                )
            ], md=4),

            # Colonne 3 : Taille d'effectif
            dbc.Col([
                html.Label("Taille d'effectif", className="text-muted mb-2"),
                dcc.Dropdown(
                    id="effectif-filter",
                    options=[{"label": effectif, "value": effectif} for effectif in df_societe["Effectif_def"].dropna().unique()],
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
                                html.Span(id="total-funding", className="metric-value"),  
                                html.Span("€", className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Financement Total", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  

            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(id="mean-funding", className="metric-value"),
                                html.Span("€", className="metric-symbol")
                            ], className="metric-number"),
                            html.P("Financement Moyen par entreprise", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm")
            ], md=3, className="mb-4"),  

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
            ], md=3, className="mb-4"),

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
            ], md=3, className="mb-4")  
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
                        dcc.Graph(id="funding-evolution", figure=fig1)
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4")
        ]),

], fluid=True)
])