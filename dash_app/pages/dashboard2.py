import dash
import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
from dash.dependencies import Input, Output
from dash_app.utils.data_loader import get_dataframe
from dash_app.app import app, cache
from dash_app.utils.preprocessing import preprocess_societe, preprocess_financements, filter_societe

########################################
# Chargement et prétraitement des données
########################################

@cache.memoize(timeout=300)
def load_data():
    """Charge et prétraite les données avec mise en cache"""
    df_societe = get_dataframe('societes.csv')
    df_financements = get_dataframe('financements.csv')
    
    # Prétraitement des données
    df_societe = preprocess_societe(df_societe)
    df_financements = preprocess_financements(df_financements)
    
    # Conversion des dates et calcul des années
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors='coerce')
    df_societe['annee_creation'] = df_societe['date_creation_def'].dt.year
    
    df_financements['Date dernier financement'] = pd.to_datetime(df_financements['Date dernier financement'], errors='coerce')
    df_financements['Année'] = df_financements['Date dernier financement'].dt.year
    
    # Calcul des années min/max pour le slider
    min_year = int(df_societe['annee_creation'].min()) if not df_societe.empty else 1986
    max_year = int(df_societe['annee_creation'].max()) if not df_societe.empty else 2025
    
    return df_societe, df_financements, min_year, max_year

# Chargement initial des données
df_societe, df_financements, min_year, max_year = load_data()

########################################
# Layout de la page Dashboard
########################################
layout = html.Div([
    # En-tête
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
                        html.Label("Activité principale", className="text-muted mb-2"),
                        dcc.Dropdown(
                            id="sector-filter",
                            options=[{"label": sec, "value": sec} for sec in sorted(df_societe["Activité principale"].dropna().unique())],
                            placeholder="Tous les secteurs",
                            multi=True,
                            className="mb-3"
                        )
                    ], md=4),
                    dbc.Col([
                        html.Label("Année de Création", className="text-muted mb-2"),
                        dcc.RangeSlider(
                            id="year-filter",
                            min=min_year,
                            max=max_year,
                            value=[min_year, max_year],
                            marks={i: str(i) for i in range(min_year, max_year + 1, 4)},
                            className="mb-3"
                        )
                    ], md=4),
                    dbc.Col([
                        html.Label("Taille d'effectif", className="text-muted mb-2"),
                        dcc.Dropdown(
                            id="effectif-filter",
                            options=[{"label": eff, "value": eff} for eff in sorted(df_societe["Effectif_def"].dropna().unique())],
                            placeholder="Toutes les tailles",
                            multi=True,
                            className="mb-3"
                        )
                    ], md=4)
                ])
            ])
        ], className="shadow-sm mb-4"),

        # KPI Cards
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="total-funding", className="metric-value")],
                                     className="metric-number"),
                            html.P("Financement Total", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="mean-funding", className="metric-value")],
                                     className="metric-number"),
                            html.P("Financement Moyen par entreprise", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="nbre-startup", className="metric-value")],
                                     className="metric-number"),
                            html.P("Startups créées", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([html.Span(id="pourc-leve", className="metric-value")],
                                     className="metric-number"),
                            html.P("Entreprises ayant levé des fonds", className="metric-label")
                        ], className="metric-card")
                    ])
                ], className="shadow-sm"), md=3, className="mb-4"
            )
        ]),

        # Graphiques
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Répartition des financements par typologie"),
                    dbc.CardBody([dcc.Graph(id="serie-funding")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Évolution des Financements"),
                    dbc.CardBody([dcc.Graph(id="funding-evolution")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Top 10 des entreprises ayant levé le plus de fonds"),
                    dbc.CardBody([dcc.Graph(id="top-funded")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Évolution de la création de startups par an"),
                    dbc.CardBody([dcc.Graph(id="startup-year")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Distribution des secteurs d'activités"),
                    dbc.CardBody([dcc.Graph(id="top-sector")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Distribution des startups par effectif (TOP 5)"),
                    dbc.CardBody([dcc.Graph(id="top-startup-size")])
                ], className="shadow-sm"), md=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Top 10 des sous-catégories de mots-clés"),
                    dbc.CardBody([dcc.Graph(id="top-subcategories")])
                ], className="shadow-sm"), md=12, className="mb-4"
            )
        ])
    ], fluid=True)
])

########################################
# Callbacks
########################################

@app.callback(
    [Output("total-funding", "children"),
     Output("mean-funding", "children"),
     Output("nbre-startup", "children"),
     Output("pourc-leve", "children"),
     Output("funding-evolution", "figure"),
     Output("serie-funding", "figure"),
     Output("top-funded", "figure"),
     Output("startup-year", "figure"),
     Output("top-sector", "figure"),
     Output("top-startup-size", "figure"),
     Output("top-subcategories", "figure")],
    [Input("sector-filter", "value"),
     Input("year-filter", "value"),
     Input("effectif-filter", "value")]
)
@cache.memoize(timeout=60)
def update_dashboard(sector, year_range, effectif):
    """Met à jour le dashboard avec les filtres sélectionnés"""
    try:
        # Filtrage des données
        df_soc = df_societe.copy()
        if sector or effectif or year_range:
            df_soc = filter_societe(df_soc, sector, effectif, year_range)
        
        # Jointure avec les financements
        df_fin = pd.merge(
            df_financements,
            df_soc[['entreprise_id', 'nom']],
            on='entreprise_id',
            how='inner'
        )
        
        # Calcul des KPIs
        total_funding = df_fin["Montant_def"].sum()
        unique_funded = df_fin["entreprise_id"].nunique()
        mean_funding = total_funding / unique_funded if unique_funded > 0 else 0
        total_startups = len(df_soc)
        percent_funded = (unique_funded / total_startups * 100) if total_startups > 0 else 0

        # 1. Évolution des financements
        funding_by_year = df_fin.groupby("Année")["Montant_def"].sum().reset_index()
        fig_evolution = px.line(
            funding_by_year,
            x="Année",
            y="Montant_def",
            title="Évolution des financements par année",
            labels={"Montant_def": "Montant total (€)", "Année": "Année"},
            template="plotly_white"
        )

        # 2. Répartition par série
        series_dist = df_fin["Série"].value_counts()
        fig_series = px.bar(
            x=series_dist.index,
            y=series_dist.values,
            title="Distribution des séries de financement",
            labels={"x": "Série", "y": "Nombre de financements"},
            template="plotly_white"
        )

        # 3. Top entreprises financées
        top_companies = df_fin.groupby(["entreprise_id", "nom"])["Montant_def"].sum()\
                             .reset_index().nlargest(10, "Montant_def")
        fig_top = px.bar(
            top_companies,
            x="nom",
            y="Montant_def",
            title="Top 10 des entreprises par montant levé",
            labels={"nom": "Entreprise", "Montant_def": "Montant total (€)"},
            template="plotly_white"
        )
        fig_top.update_layout(xaxis_tickangle=45)

        # 4. Évolution création startups
        startups_by_year = df_soc.groupby("annee_creation")["entreprise_id"].count().reset_index()
        fig_startups = px.line(
            startups_by_year,
            x="annee_creation",
            y="entreprise_id",
            title="Évolution du nombre de startups créées par année",
            labels={"annee_creation": "Année", "entreprise_id": "Nombre de startups"},
            template="plotly_white"
        )

        # 5. Distribution secteurs
        sector_dist = df_soc["Activité principale"].value_counts().nlargest(10)
        fig_sectors = px.bar(
            x=sector_dist.values,
            y=sector_dist.index,
            title="Top 10 des secteurs d'activité",
            labels={"x": "Nombre d'entreprises", "y": "Secteur"},
            template="plotly_white",
            orientation='h'
        )

        # 6. Distribution par taille
        size_dist = df_soc["Effectif_def"].value_counts().nlargest(5)
        fig_size = px.pie(
            values=size_dist.values,
            names=size_dist.index,
            title="Distribution des effectifs",
            template="plotly_white"
        )

        # 7. Top sous-catégories
        all_subcategories = []
        for cats in df_soc["Sous-Catégorie"].fillna("").astype(str).str.split("|"):
            if isinstance(cats, list):
                all_subcategories.extend([cat.strip() for cat in cats if cat.strip() and cat.strip() != 'nan'])
        
        subcategory_counts = pd.Series(all_subcategories).value_counts().nlargest(10)
        fig_subcategories = px.bar(
            x=subcategory_counts.values,
            y=subcategory_counts.index,
            title="Top 10 des sous-catégories",
            labels={"x": "Nombre d'entreprises", "y": "Sous-catégorie"},
            template="plotly_white",
            orientation='h'
        )

        # Formatage des KPIs
        formatted_total = f"{total_funding:,.0f} €".replace(",", " ")
        formatted_mean = f"{mean_funding:,.0f} €".replace(",", " ")
        formatted_startups = f"{total_startups:,}".replace(",", " ")
        formatted_percent = f"{percent_funded:.1f}%"

        return (formatted_total, formatted_mean, formatted_startups, formatted_percent,
                fig_evolution, fig_series, fig_top, fig_startups, fig_sectors, 
                fig_size, fig_subcategories)
    except Exception as e:
        print(f"Erreur dans update_dashboard: {str(e)}")
        # Retourne des valeurs par défaut en cas d'erreur
        empty_fig = px.scatter(title="Aucune donnée disponible")
        return ("0 €", "0 €", "0", "0%",
                empty_fig, empty_fig, empty_fig, empty_fig, empty_fig,
                empty_fig, empty_fig)