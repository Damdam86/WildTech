import streamlit as st
import pandas as pd
import os

st.write("Répertoire courant :", os.getcwd())

# Charger le fichier CSV
df = pd.read_csv('./merged_df.csv', sep=';')

# Ajouter un sélecteur pour choisir une entreprise
selected_company = st.selectbox("Sélectionnez une entreprise :", df['nom'].dropna().unique())

# Filtrer les données pour l'entreprise sélectionnée
company_data = df[df['nom'] == selected_company].iloc[0]  # Prend la première ligne correspondante

# Afficher les informations de l'entreprise sélectionnée
st.markdown(f"## Informations pour l'entreprise **{selected_company}**")

# Disposition en colonnes
col1, col2 = st.columns([5, 2])

with col1:
    st.image(company_data['logo_x'], caption=f"Logo de {selected_company}", use_container_width=True)
    st.markdown(f"### Description :")
    st.write(company_data['description_x'])

with col2:
    st.markdown(f"### Site Web :")
    if pd.notna(company_data['site_web_x']):
        st.markdown(f"[Visitez le site]( {company_data['site_web_x']} )")
    else:
        st.write("Site non disponible")
    
    st.markdown("### Hashtags :")
    st.write(", ".join(company_data['mots_cles_x']) if isinstance(company_data['mots_cles_x'], list) else "Aucun hashtag disponible.")

# Produits et secteurs
st.markdown("### Produits et secteurs :")
st.write("**Produits :**", ", ".join(company_data['product_types']) if isinstance(company_data['product_types'], list) else "Non spécifié")
st.write("**Secteurs :**", ", ".join(company_data['sectors']) if isinstance(company_data['sectors'], list) else "Non spécifié")

# Financements
st.markdown("### Informations supplémentaires :")
st.write(f"**Financement total :** {company_data['Montant'] if pd.notna(company_data['Montant']) else 'Non spécifié'}")
