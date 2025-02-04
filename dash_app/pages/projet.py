from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from dash import callback

app = Dash()

layout = dbc.Container([

    # Titre principal
    html.H1("Notre projet", className="text-center my-4"),
    
    html.H6("Ce projet est une vraie aventure dans le monde des données ! Vous allez créer de A à Z votre propre application d’analyse de données. Le plus cool ? C’est VOUS qui choisissez votre sujet et vos sources de données. Que vous soyez passionné(e) par :"),
    
    dcc.Markdown('''
    * Le business et l’analyse de marchés
    * La production industrielle et l’optimisation
    * La finance et les tendances économiques
    * Le sport et ses performances
    * L’environnement et le développement durable
    * Les médias sociaux et le marketing digital
    * La musique ou le cinéma
    * Les jeux vidéo et le gaming … ou tout autre domaine qui vous intéresse, vous pourrez orienter votre projet dans cette direction !'''),
    html.H6("Bien sûr, pour que tout le monde puisse avancer ensemble et s’entraider, on suivra les mêmes étapes et la même structure de projet. Vous devez respecter le workflow suivant : collecter des données, les transformer, les analyser et créer des tableaux de bord visuels, vous êtes des DATA-Analystes maintenant, vous savez de quoi on parle. Et pour rendre tout ça encore plus intéressant, on utilisera même l’IA pour enrichir vos analyses !L’idée est simple : vous êtes libre de laisser parler votre créativité sur le QUOI, pendant qu’on vous guide sur le COMMENT. Que vous visiez un projet purment professionnel ou qui joint le personnel, c’est l’occasion de développer des compétences concrètes sur un sujet qui vous tient à cœur ! 🚀", className="text-left my-4")

 ], fluid=True)