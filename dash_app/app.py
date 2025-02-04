import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from pages import home, dashboard  # Importer les pages

# Initialisation de l'application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG], suppress_callback_exceptions=True)

# Barre de navigation
navbar = dbc.NavbarSimple(
    brand="StartHub",
    brand_href="/",
    color="primary",
    dark=True,
    children=[
        dbc.NavItem(dbc.NavLink("Accueil", href="/home")),
        dbc.NavItem(dbc.NavLink("Dashboard", href="/dashboard"))
    ])

# Layout
app.layout = html.Div([
    navbar,
    dcc.Location(id='url', refresh=False),  # Location pour suivre l'URL
    html.Div(id='page-content')  # Contenu de la page à changer en fonction de l'URL
])

# Callback pour changer de page en fonction de l'URL
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/dashboard':
        return dashboard.layout
    else:
        return home.layout

if __name__ == '__main__':
    app.run_server(debug=True)