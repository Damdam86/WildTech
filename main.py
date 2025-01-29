import streamlit as st
import pandas as pd
import altair as alt

# Titre principal
st.title("Startups France - Dashboard")

# Barre de recherche (facultative)
search = st.text_input("Rechercher une startup...")

# Indicateurs clés (dans des colonnes)
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Startups", "2,543")
with col2:
    st.metric("Levées de fonds (mois)", "28")
with col3:
    st.metric("Fondateurs", "3,721")
with col4:
    st.metric("Croissance (mois)", "+12.3%")

# Données d'exemple pour les levées de fonds par mois
df = pd.DataFrame({
    'Mois': ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Juin'],
    'Levées de fonds': [8, 18, 14, 25, 30, 22]  # en millions, par exemple
})

# Titre du graphique
st.subheader("Levées de fonds par mois")

# Création du graphique avec Altair
chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x='Mois',
        y='Levées de fonds',
        tooltip=['Mois', 'Levées de fonds']
    )
    .properties(width='container', height=300)
)

# Affichage du graphique
st.altair_chart(chart, use_container_width=True)
