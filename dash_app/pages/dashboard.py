from dash import Dash, html, dcc, Output, Input
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback
from app import get_dataframe  # Importer app et la fonction get_dataframe


# Chargement des données
df = get_dataframe('societes.csv')
df_financement = get_dataframe('financements.csv')
df_personne = get_dataframe('personnes.csv')

layout = dbc.Container([

    #Titre principal
    html.H1("Dashboard", className="text-center my-4"),

    # Sélecteur temporel
     dcc.DatePickerRange(
        id='date-picker',
        min_date_allowed=pd.to_datetime(df['date_creation_def']).min(),
        max_date_allowed=pd.to_datetime(df['date_creation_def']).max(),
        start_date=pd.to_datetime("2000-01-01"),
        end_date=pd.to_datetime(df['date_creation_def']).max(),
        display_format='YYYY-MM-DD',
        className="mb-4"
    ),

    #Boutons pour afficher différentes vues
    dbc.ButtonGroup([
        dbc.Button("Start-ups", id="btn-startups", color="primary", n_clicks=0),
        dbc.Button("Financements", id="btn-financements", color="info", n_clicks=0),
        dbc.Button("Personnes", id="btn-personnes", color="success", n_clicks=0),
    ], className="mb-4 d-flex justify-content-center"),

    #Zone d'affichage dynamique
    html.Div(id="dynamic-content"),

], fluid=True)

#Callback pour mettre à jour le contenu dynamique
@callback(
    Output("dynamic-content", "children"),
    [
        Input("btn-startups", "n_clicks"),
        Input("btn-financements", "n_clicks"),
        Input("btn-personnes", "n_clicks"),
        Input("date-picker", "start_date"),
        Input("date-picker", "end_date")
    ]
)
def update_content(n1, n2, n3, start_date, end_date):
    ctx = dash.callback_context
    if not ctx.triggered:
        return html.P("Sélectionnez une vue avec les boutons ci-dessus.")

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Filtrer les start-ups selon la date sélectionnée
    df_filtered = df[(df["date_creation_def"] >= start_date) & (df["date_creation_def"] <= end_date)]

    if button_id == "btn-startups":
        return dbc.Card([
            dbc.CardBody([
                html.H4("Nombre de Start-ups", className="text-center"),
                html.H2(f"{df_filtered.nom.nunique()}", className="text-center")
            ])
        ], className="mb-4")
    
    elif button_id == "btn-financements":
        return html.P("Affichage des financements en construction...")  # Ajouter un graphique ici

    elif button_id == "btn-personnes":
        return html.P("Affichage des personnes clés en construction...")  # Ajouter un graphique ici

    return html.P("Sélectionnez une vue avec les boutons.")