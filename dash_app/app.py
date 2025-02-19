import os
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import html, dcc
from flask_caching import Cache
from dash_app.utils.data_loader import get_dataframe, ESSENTIAL_COLUMNS
from dash_app.utils.preprocessing import preprocess_societe, preprocess_financements, filter_societe

# Configuration de l'application Dash avec optimisations
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    update_title=None,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ]
)

server = app.server
server.config['PROPAGATE_EXCEPTIONS'] = True

# Configuration du cache avec des paramètres optimisés
CACHE_CONFIG = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': '/tmp/dash_cache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_THRESHOLD': 500
}
cache = Cache(server, config=CACHE_CONFIG)

# Importation des pages
from dash_app.pages import home, projet, dashboard2, map, equipe, amelioration

# Barre de navigation
navbar = dbc.NavbarSimple(
    brand="StartHub",
    brand_href="/",
    color="primary",
    dark=True,
    children=[
        dbc.NavItem(dbc.NavLink("Equipe", href="/equipe")),
        dbc.NavItem(dbc.NavLink("Projet", href="/projet")),
        dbc.NavItem(dbc.NavLink("Accueil", href="/home")),
        dbc.NavItem(dbc.NavLink("Dashboard", href="/dashboard2")),
        dbc.NavItem(dbc.NavLink("Carte", href="/map")),
        dbc.NavItem(dbc.NavLink("Amélioration", href="/amelioration"))
    ]
)

# Layout principal
app.layout = html.Div([
    navbar,
    dcc.Location(id="url", refresh=False),
    html.Div(id="page-content")
])

# Callback pour changer de page
@app.callback(
    dash.dependencies.Output("page-content", "children"),
    [dash.dependencies.Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/dashboard2":
        return dashboard2.layout
    elif pathname == "/projet":
        return projet.layout
    elif pathname == "/equipe":
        return equipe.layout
    elif pathname == "/amelioration":
        return amelioration.layout
    elif pathname == "/map":
        return map.layout
    elif pathname == "/" or pathname == "/home":
        return home.layout
    else:
        return html.Div([
            html.H1("404: Page non trouvée", className="text-center mt-5"),
            html.P("La page que vous recherchez n'existe pas.", className="text-center")
        ])

if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8000)