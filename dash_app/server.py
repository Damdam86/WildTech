# server.py
import dash

app = dash.Dash(__name__)
server = app.server  # Si vous avez besoin du serveur Flask

# Vous pouvez ajouter ici des configurations globales, etc.