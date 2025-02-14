import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from flask_caching import Cache
from io import StringIO
import plotly.express as px
import plotly.graph_objects as go


# Initialisation de l'application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

#Configuration du cache
cache = Cache(app.server, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': 'cache-directory'})
TIMEOUT = None #cache permanent jusqu'à redémarrage de l'app 

#Chargement des csv
@cache.memoize(timeout=TIMEOUT)
def query_all_data():
    files = ["societes.csv", "financements.csv", "personnes.csv"]  # Liste des fichiers à charger
    dataframes = {}

    for file in files:
        df = pd.read_csv(f'assets/{file}')
        dataframes[file] = df.to_json(date_format='iso', orient='split')  # Stockage JSON
    return dataframes  # Retourne un dictionnaire JSON

def get_dataframe(filename):
    dataframes = query_all_data()  # Récupère tous les datasets en cache
    json_string = dataframes[filename]  # Récupère la chaîne JSON
    return pd.read_json(StringIO(json_string), orient='split')  # Convertit en DataFrame en utilisant StringIO pour le FutureWarning

from pages import home, projet, dashboard2, map, equipe, amelioration # Importer les pages

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
    if pathname == '/dashboard2':
        return dashboard2.layout
    elif pathname == '/projet':
        return projet.layout
    elif pathname == '/equipe':
        return equipe.layout
    elif pathname == '/amelioration':
        return amelioration.layout
    elif pathname == '/map':
        return map.layout
    else:
        return home.layout

@app.callback(
    Output("mean-funding", "children"),  
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ]  
)
def mean_funding(sector, year_range, effectif):
    df = get_dataframe("financements.csv")  
    df['Montant_def'] = pd.to_numeric(df['Montant_def'], errors='coerce')  # Conversion en float

    # Charger les données sociétés pour le filtre sur l'activité et l'effectif
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[(df_societe["annee_creation"] >= year_range[0]) & 
                                (df_societe["annee_creation"] <= year_range[1])]

    # Filtrer les entreprises en fonction des sociétés filtrées
    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]

    # Calcul du financement moyen
    if df["entreprise_id"].nunique() > 0:
        mean_funding = df['Montant_def'].fillna(0).sum() / df["entreprise_id"].nunique()
    else:
        mean_funding = 0  # Évite la division par zéro

    return f"{mean_funding:,.0f} €".replace(",", " ")


@app.callback(
    Output("total-funding", "children"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ]  
)
def total_funding(sector, year_range, effectif):
    df = get_dataframe("financements.csv")  
    df_societe = get_dataframe("societes.csv")  # Charge une seule fois

    # Conversion des colonnes
    df['Montant_def'] = pd.to_numeric(df['Montant_def'], errors='coerce')  
    df_societe['date_creation_def'] = pd.to_datetime(
        df_societe['date_creation_def'], 
        errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  #Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    elif effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)] 
    elif year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # évite les NaN
            (df_societe["annee_creation"].between(year_range[0], year_range[1]))
        ]

    # Filtrer les entreprises en fonction des sociétés filtrées
    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]

    # Calcul du financement total
    total_funding = df['Montant_def'].fillna(0).sum()

    return f"{total_funding:,.0f} €".replace(",", " ")

#Callback pour rendre le graph funding dynamique
@app.callback(
    Output("funding-evolution", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def update_funding_graph(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 

    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year 

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[
            (df["Année"].notna()) &  # Évite les NaN
            (df["Année"].between(year_range[0], year_range[1]))
        ]

    # Filtre sur les financements en fonction des sociétés sélectionnées
    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])] 

    funding_by_year = df.groupby('Année')['Montant_def'].sum().reset_index()
    fig1 = px.line(funding_by_year, x='Année', y='Montant_def')

    return fig1

@app.callback(
    Output("serie-funding", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def update_series_graph(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # Évite les NaN
            (df_societe["annee_creation"].between(year_range[0], year_range[1])
             )
        ]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]
    funding_by_series = df["Série"].value_counts().nlargest(10).reset_index()
    fig2 = px.bar(funding_by_series, x='index', y='Série')

    return fig2

@app.callback(
    Output("startup-year", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def startup_per_year(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # Évite les NaN
            (df_societe["annee_creation"].between(1986, year_range[1]))
        ]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]  
    startups_by_year = df_societe.groupby('annee_creation').agg({'entreprise_id': 'count'}).reset_index()
    startups_by_year.columns = ['annee_creation', 'nombre_startups'] 

    fig3 = px.line(startups_by_year, x='annee_creation', y='nombre_startups')

    return fig3

@app.callback(
    Output("top-funded", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def top_funded(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  #Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]  #Comparaison directe
    if year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # Évite les NaN
            (df_societe["annee_creation"].between(year_range[0], year_range[1])
             )
        ]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]
    
    top_funded_companies = df.groupby("entreprise_id")["Montant_def"].sum().nlargest(10).reset_index()
    top_funded_companies = top_funded_companies.merge(df_societe, on="entreprise_id", how="left")
    fig4 = px.bar(top_funded_companies, x='nom', y='Montant_def')

    return fig4


@app.callback(
    Output("pourc-leve", "children"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def pourc_levee(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # Évite les NaN
            (df_societe["annee_creation"].between(year_range[0], year_range[1])
             )
        ]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]
    
    # Calcul de la part des entreprises ayant levé des fonds
    nb_total_entreprises = df_societe.shape[0]
    nb_entreprises_funded = df["Montant_def"].nunique()
    part_funded = (nb_entreprises_funded / nb_total_entreprises) * 100

    return f"{part_funded:.2f}%"


@app.callback(
    Output("nbre-startup", "children"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])
def nbre_startup(sector, year_range, effectif):
    df = get_dataframe("financements.csv") 
    df['Date dernier financement'] = pd.to_datetime(df['Date dernier financement'], errors='coerce')
    df['Année'] = df['Date dernier financement'].dt.year
    df['Montant_def'] = pd.to_numeric(df["Montant_def"], errors='coerce').fillna(0) 
    df_societe = get_dataframe("societes.csv")
    df_societe['date_creation_def'] = pd.to_datetime(df_societe['date_creation_def'], errors="coerce")
    df_societe["annee_creation"] = df_societe["date_creation_def"].dt.year  # Extraire l'année

    # Appliquer les filtres
    if sector:
        df_societe = df_societe[df_societe["Activité principale"].isin(sector)]
    if effectif:
        df_societe = df_societe[df_societe["Effectif_def"].isin(effectif)]
    if year_range:
        df_societe = df_societe[
            (df_societe["annee_creation"].notna()) &  # Évite les NaN
            (df_societe["annee_creation"].between(year_range[0], year_range[1]))
        ]

    df = df[df["entreprise_id"].isin(df_societe["entreprise_id"])]

    # Calcul du nombre unique de startups
    nbre_start = df_societe['nom'].nunique()

    # Formater avec un espace comme séparateur de milliers
    formatted_nbre_start = f"{nbre_start:,}".replace(",", " ")

    return formatted_nbre_start



@app.callback(
    Output("top-sector", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def top_sector(sector, year_range, effectif):
    df = get_dataframe("societes.csv")
    df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
    df["annee_creation"] = df["date_creation_def"].dt.year  # Extraire l'année
    # Dictionnaire des secteurs d'activité (codes NAF partiels)
    dict_secteurs = {
        '01': 'Agriculture',
        '02': 'Sylviculture et exploitation forestière',
        '03': 'Pêche et aquaculture',
        '05': 'Extraction de houille et de lignite',
        '08': 'Autres industries extractives',
        '10': 'Industries alimentaires',
        '13': 'Fabrication de textiles',
        '18': 'Imprimerie et reproduction d\'enregistrements',
        '26': 'Fabrication de produits informatiques et électroniques',
        '32': 'Autres industries manufacturières',
        '41': 'Construction de bâtiments',
        '43': 'Travaux de construction spécialisés',
        '49': 'Transports terrestres et transport par conduites',
        '56': 'Restauration',
        '58': 'Édition',
        '61': 'Télécommunications',
        '62': 'Programmation, conseil et autres activités informatiques',
        '63': 'Services d’information',
        '64': 'Activités financières et d’assurance',
        '68': 'Activités immobilières',
        '70': 'Activités des sièges sociaux, conseil en gestion',
        '71': 'Ingénierie et études techniques',
        '72': 'Recherche et développement scientifique',
        '73': 'Publicité et études de marché',
        '74': 'Autres activités spécialisées, scientifiques et techniques',
        '77': 'Location et exploitation de biens immobiliers',
        '82': 'Activités administratives et autres services de soutien'
    }
    # Étape 1 : Extraire les 2 premiers chiffres du code de l'activité principale
    df['Secteur'] = df['Activité principale'].str[:2]
    # Étape 2 : Mapper avec le dictionnaire des secteurs
    df['Nom Secteur'] = df['Secteur'].map(dict_secteurs)
    # Calculer la distribution des valeurs et trier par ordre décroissant
    
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[
            (df["annee_creation"].notna()) &  # Évite les NaN
            (df["annee_creation"].between(year_range[0], year_range[1]))
        ]

    distribution = df['Nom Secteur'].value_counts().reset_index()
    distribution.columns = ['Nom Secteur', 'Count']
    distribution_top5 = distribution.sort_values(by='Count', ascending=True).tail(5)

    # Créer le graphique en barres
    fig5 = px.bar(distribution_top5, x='Count', y='Nom Secteur',
                labels={'Nom Secteur': 'Secteur', 'Count': 'Nombre'},
                text='Count')

    return fig5


@app.callback(
    Output("top-startup-size", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def top_startup_size(sector, year_range, effectif):
    df = get_dataframe("societes.csv")
    df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
    df["annee_creation"] = df["date_creation_def"].dt.year  # Extraire l'année
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[
            (df["annee_creation"].notna()) &  # Évite les NaN
            (df["annee_creation"].between(year_range[0], year_range[1]))
        ]

    # Calculer la distribution des valeurs et trier par ordre décroissant
    distribution = df['Effectif_def'].value_counts().reset_index()
    distribution.columns = ['Effectif', 'Count']
    # Filtrer pour afficher uniquement le TOP 5
    distribution_top5 = distribution.sort_values(by='Count', ascending=True).tail(5)
    # Créer le graphique en camembert
    fig6 = px.pie(distribution_top5, names='Effectif', values='Count')

    return fig6

@app.callback(
    Output("cloud-words", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ])

def top_startup_size(sector, year_range, effectif):
    from wordcloud import WordCloud
    from collections import Counter
    import plotly.graph_objects as go

    df = get_dataframe("societes.csv")
    df['date_creation_def'] = pd.to_datetime(df['date_creation_def'], errors="coerce")
    df["annee_creation"] = df["date_creation_def"].dt.year  # Extraire l'année
    if sector:
        df = df[df["Activité principale"].isin(sector)]
    if effectif:
        df = df[df["Effectif_def"].isin(effectif)]
    if year_range:
        df = df[
            (df["annee_creation"].notna()) &  # Évite les NaN
            (df["annee_creation"].between(year_range[0], year_range[1]))
        ]

    df['mots_cles_def'] = df['mots_cles_def'].fillna('')
    # Fusionner les mots-clés de toutes les lignes en une seule grande chaîne
    all_keywords = ','.join(df['mots_cles_def'].astype(str))
    # Diviser les mots-clés en une liste de mots individuels
    keywords_list = [keyword.strip() for keyword in all_keywords.split(',') if keyword.strip()]
    # Utiliser Counter pour compter la fréquence des mots-clés
    keywords_freq = Counter(keywords_list)
    # Convertir le résultat en DataFrame trié
    keywords_df = pd.DataFrame(keywords_freq.items(), columns=['Mot', 'Count']).sort_values(by='Count', ascending=False)
    # Créer un dictionnaire des mots avec leurs fréquences
    word_freq = dict(zip(keywords_df['Mot'], keywords_df['Count']))
    # Générer le nuage de mots
    wordcloud = WordCloud(width=1000, height=600, background_color='white', colormap='viridis').generate_from_frequencies(word_freq)
    img = wordcloud.to_array()

    # Créer une figure Plotly avec l'image du nuage de mots
    fig = go.Figure()
    fig.add_trace(
        go.Image(z=img)
    )
    fig.update_layout(
        margin={"r": 10, "t": 40, "l": 10, "b": 10},
        xaxis=dict(visible=False),  # Cacher l'axe X
        yaxis=dict(visible=False)   # Cacher l'axe Y
    )

    return fig

#Graph catégories: 
@app.callback(
    Output("top-subcategories", "figure"),
    [
        Input('sector-filter', 'value'),
        Input('year-filter', 'value'),
        Input('effectif-filter', 'value')
    ]
)
def update_top_subcategories(sector, year_range, effectif):
    df2 = get_dataframe("societes.csv") 
    
    df2['date_creation_def'] = pd.to_datetime(df2['date_creation_def'], errors="coerce")
    df2["annee_creation"] = df2["date_creation_def"].dt.year

    if sector:
        df2 = df2[df2["Activité principale"].isin(sector)]
    if effectif:
        df2 = df2[df2["Effectif_def"].isin(effectif)]
    if year_range:
        df2 = df2[
            (df2["annee_creation"].notna()) &
            (df2["annee_creation"].between(year_range[0], year_range[1]))
        ]

    df2 = df2[df2['Sous-Catégorie'] != 0] 
    top_years = df2.loc[df2["Sous-Catégorie"] != "Divers", "Sous-Catégorie"].value_counts().nlargest(10)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top_years.sort_values(ascending=True).values,
        y=top_years.sort_values(ascending=True).index.astype(str),
        orientation='h',
        marker=dict(color='royalblue'),
    ))

    fig.update_layout(
        xaxis_title="Fréquence",
        yaxis_title="Sous-catégorie de mots-clés",
        yaxis=dict(categoryorder='total ascending')
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
