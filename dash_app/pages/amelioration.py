import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Layout de la page Amélioration
layout = html.Div([
    # Hero Section
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
                            html.H4("Actualisation des données", className="metric-value"),
                            html.P("Une actualisation automatique / trigger sur les levées de fonds, les nouvelles startups et la performance financière permettrait de consolider la base de données.", className="text-muted"),
                        ], className="step-card-content")
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("2", className="step-number"),
                            html.H4("Suivi des performances au fil du temps", className="metric-value"),
                            html.P("Comme l'évolution des revenus, des levées de fonds, ou de la taille de l’équipe, ce suivi pourrait permettre d'afficher des nouveaux KPI intéressants sur les startups en France.", className="text-muted"),
                        ], className="step-card-content")
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("3", className="step-number"),
                            html.H4("Gestion des catégories d'entreprises", className="metric-value"),
                            html.P("Une évolution serait de pouvoir rechercher / afficher les entreprises par catégories. Par manque de temps nous n'avons pas pu mettre en place cette fonctionnalité.", className="text-muted"),
                        ], className="step-card-content")
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4"),
                dbc.Col([
                    html.Div([
                        html.Div([
                            html.Span("4", className="step-number"),
                            html.H4("Nettoyage des données", className="metric-value"),
                            html.P("Il faudrait sûrement reprendre le pipeline. C'était une première et il faudrait revoir l'ordre des tâches, créer de nouvelles tâches, etc., pour améliorer le flow et le nettoyage.", className="text-muted"),
                        ], className="step-card-content")
                    ], className="step-card")
                ], md=6, lg=3, className="mb-4")
            ]),
            dbc.Row([
                dbc.Col([
                    html.P(
                        "Ces améliorations permettraient d'obtenir des informations actualisées sur les tendances du marché.",
                        className="section-description text-center", style={'font-size': '18px'}
                    )
                ], md=12)
            ])
        ], className="improvements-section mb-5")
    ], fluid=True, className="project-page")
])
