import dash
import pandas as pd
import missingno as msno
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash_app.utils.data_loader import get_dataframe

df = get_dataframe('societes.csv')

layout = html.Div([
    # Hero Section
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("Le Projet", className="hero-title mb-4"),
                    html.H5("Une analyse approfondie de l'écosystème startup français basée sur plus de 10,000 entreprises", className="hero-subtitle mb-4")
                ], md=8, lg=6)
            ], className="min-vh-75 align-items-center")
        ], fluid=True)
    ], className="hero-section mb-5"),

    dbc.Container([
        # Section Objectifs Pédagogiques
        html.Div([
            html.H2("Objectifs Pédagogiques", className="section-title text-center mb-5"),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("Collecte de données", className="metric-value")], className="metric-number"),
                        html.P("Acquisition de données depuis diverses sources (APIs, fichiers, bases de données).", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("Cleaning - pipeline", className="metric-value")], className="metric-number"),
                        html.P("Application de techniques de traitement des données avec Python. Conception et mise en œuvre d’un processus d’Extraction, Transformation et Chargement.", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("Visualisation et reporting ", className="metric-value")], className="metric-number"),
                        html.P("Création d'un tableau de bord dynamique et interactif.", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("IA et enrichissement", className="metric-value")], className="metric-number"),
                        html.P("Exploration des apports de l’intelligence artificielle dans l’analyse des données.", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4")
            ]),
            html.H2("Métriques Clés", className="section-title text-center mb-5"),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("10", className="metric-value"), html.Br(),
                                  html.Span("sources différentes", className="metric-symbol")], className="metric-number"),
                        html.P("Pour la création de la base", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("3 API", className="metric-value"), html.Br(),
                                  html.Span("SIREN, Recherche Société et OPENAI", className="metric-symbol")], className="metric-number"),
                        html.P("Pour compléter les informations", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("+4850", className="metric-value"), html.Br(),
                                  html.Span("mots clés", className="metric-symbol")], className="metric-number"),
                        html.P("Pour rechercher dans la base", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([html.Span("10k+", className="metric-value"), html.Br(),
                                  html.Span("entreprises", className="metric-symbol")], className="metric-number"),
                        html.P("Dans notre base de données", className="metric-label")
                    ], className="metric-card")
                ], md=6, lg=3, className="mb-4")
            ])
        ], className="mb-5"),
        # Section Notre Approche
        html.Div([
            html.H2("Notre Approche", className="section-title text-center mb-5", id="methodology"),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("1", className="step-number"),
                            html.H4("Collecte de Données", className="metric-value"),
                            html.P("Scraping depuis 10 sources", className="text-muted"),
                            html.Ul([
                                html.Li("Analyse des APIs disponibles"),
                                html.Li("Analyse des sites web dédiés aux startups (FrenchTech, BPI, etc.)"),
                                html.Li("Utilisation de Selenium dans 90% des cas"),
                                html.Li("Les scraping ne sont pas automatisés"),
                                html.Li("Difficilement 'automatisable'")
                            ], className="step-list")
                        ])
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("2", className="step-number"),
                            html.H4("Nettoyage", className="metric-value"),
                            html.P("Traitement et standardisation des données", className="text-muted"),
                            html.Ul([
                                html.Li("Normalisation des formats"),
                                html.Li("Explosion des colonnes"),
                                html.Li("Fusion des mots clés, adresse, site web"),
                                html.Li("Création d'un pipeline automatisé sous Prefect"),
                                html.Li("Envoi des CSV nettoyés sur un bucket S3")
                            ], className="step-list")
                        ])
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("3", className="step-number"),
                            html.H4("Analyse", className="metric-value"),
                            html.P("Modélisation et analyse exploratoire", className="text-muted"),
                            html.Ul([
                                html.Li("Détection des tendances"),
                                html.Li("Regroupement par industries"),
                                html.Li("Analyse des financements")
                            ], className="step-list")
                        ])
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("4", className="step-number"),
                            html.H4("Visualisation", className="metric-value"),
                            html.P("Création d'un dashboard et d'une interface", className="text-muted"),
                            html.Ul([
                                html.Li("Dash pour l'interface"),
                                html.Li("Plotly pour les visualisations"),
                                html.Li("Déploiement sur serveur cloud")
                            ], className="step-list")
                        ])
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4")
            ])
        ], className="mb-5"),
        # Section Technologies
        html.Div([
            html.H2("Technologies utilisées", className="section-title text-center mb-5"),
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H4("Backend", className="metric-value"),
                        html.Div([
                            dbc.Badge("Python", color="primary", className="me-2 mb-2"),
                            dbc.Badge("FastAPI", color="success", className="me-2 mb-2"),
                            dbc.Badge("OpenAI", color="secondary", className="me-2 mb-2")
                        ])
                    ], className="tech-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.H4("Scraping", className="metric-value"),
                        html.Div([
                            dbc.Badge("Pandas", color="primary", className="me-2 mb-2"),
                            dbc.Badge("BeautifulSoup", color="secondary", className="me-2 mb-2"),
                            dbc.Badge("Selenium", color="success", className="me-2 mb-2")
                        ])
                    ], className="tech-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.H4("Data Processing", className="metric-value"),
                        html.Div([
                            dbc.Badge("Pandas", color="primary", className="me-2 mb-2"),
                            dbc.Badge("NumPy", color="success", className="me-2 mb-2"),
                            dbc.Badge("Scikit-learn", color="info", className="me-2 mb-2"),
                            dbc.Badge("Prefect", color="warning", className="me-2 mb-2"),
                            dbc.Badge("AWS S3", color="secondary", className="me-2 mb-2"),
                            dbc.Badge("Fuzzywuzzy", color="red", className="me-2 mb-2")
                        ])
                    ], className="tech-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.H4("Visualisation", className="metric-value"),
                        html.Div([
                            dbc.Badge("Plotly", color="primary", className="me-2 mb-2"),
                            dbc.Badge("Dash", color="success", className="me-2 mb-2")
                        ])
                    ], className="tech-card")
                ], md=6, lg=3, className="mb-4")
            ])
        ], className="mb-5"),
        # Section Base de Données
        html.Div([
            html.H2("Notre base de données", className="section-title text-center mb-5"),
            html.Img(src="/assets/data_def.png", style={"width": "100%"})
        ], className="project-page bg-white py-5")
    ], fluid=True)
])
