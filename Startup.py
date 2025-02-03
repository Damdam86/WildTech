from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

# Incorporate data 
df = pd.read_csv('merged_df.csv', sep=';')

# Initialize the app with a Bootstrap(for a better layout)
app = Dash(__name__, external_stylesheets=[dbc.themes.YETI])

# App layout
app.layout = dbc.Container([
    #Title
    html.H1(children='StartHub', style={'textAlign':'center', 'font-family': 'Arial, sans-serif'}),

    #KPI cards
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Start-ups", className="card-title", style={'textAlign':'center'}),
                html.H2(f"{df.nom.nunique()}", className="card-text", style={'textAlign':'center'}),
            ])
        ],), width=4),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H4("Domaines d'activités", className="card-title", style={'textAlign':'center'}),
                html.H2(f"{df.tags.nunique()}", className="card-text", style={'textAlign':'center'}),
            ])
        ],), width=4) 

    ], justify="center", className="mb-4"), #ClassName : pour l'espacement entre la carte et la selectbox
        
    #Selectbox
    dcc.Dropdown(df.nom.unique(),
                 id='df-dropdown', 
                 placeholder='Sélectionnez une start up'),

    #Startup selected info 
    html.Div(id="startup-info", style={"textAlign": "center", "fontSize": "18px"}),

], fluid= True) # occupe toute la largeur disponible, peu importe la taille de l'écran

#Callback to update informations
@app.callback(
    Output("startup-info", "children"),
    Input("df-dropdown", "value")
)

#Display Startup info
def update_startup_info(selected_startup):
    if not selected_startup:
        return ""  

    else:
        startup_data = df[df["nom"] == selected_startup].iloc[0]

        return dbc.Row([
            #Logo and Basic Info Card
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Img(src=startup_data["logo"], style={"width": "150px", "margin": "0 auto", "display": "block"}),
                        html.H3(startup_data["nom"], className="text-center mt-3"),
                        html.P(f"Année de création: {startup_data['Date de création_x']}"),
                        html.P((startup_data['mots_cles_b']), className="text-center"),
                        html.P((startup_data['tags']), className="text-center")])
                ], className="h-100"),
                width=4),
            
            # Description Card
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Description"),
                    dbc.CardBody([html.P(startup_data["description"])]),
                    dbc.CardHeader("Financement"),
                    dbc.CardBody([html.P(f"Date du dernier financement: {startup_data["Date dernier financement"]}")]),
                    dbc.CardBody([html.P(f"Montant : {startup_data["Montant"]}")])
                ], className="h-100"),
                width=4),
            
            # Contact Info Card
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader("Informations de contact"),
                    dbc.CardBody([html.P([html.Strong("Adresse: "), startup_data["adresse_z"]]),
                    html.P([html.Strong("Site web: "),
                    html.A(startup_data["site_web_x"],href=startup_data["site_web_x"])])])
                ], className="h-100"),
                width=4)], className="g-4")  # Add gap between cards
        
# Run the app
if __name__ == '__main__':
    app.run(debug=True)


 