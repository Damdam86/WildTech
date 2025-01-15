import streamlit as st
import pandas as pd

# Charger le fichier CSV
df = pd.read_csv('bpifrance_startups_data2.csv')

# Ajouter un sélecteur pour choisir une entreprise
selected_company = st.selectbox("Sélectionnez une entreprise :", df['name'].dropna().unique())

# Filtrer les données pour l'entreprise sélectionnée
company_data = df[df['name'] == selected_company].iloc[0]  # Prend la première ligne correspondante

# Afficher les informations de l'entreprise sélectionnée
st.markdown(f"## Informations pour l'entreprise **{selected_company}**")

# Disposition en colonnes
col1, col2 = st.columns(2)

with col1:
    st.image(company_data['logo'], caption=f"Logo de {selected_company}", use_column_width=True)
    st.markdown(f"### Description :")
    st.write(company_data['description'])

with col2:
    st.markdown(f"### Site Web :")
    if pd.notna(company_data['website']):
        st.markdown(f"[Visitez le site]( {company_data['website']} )")
    else:
        st.write("Site non disponible")
    
    st.markdown("### Hashtags :")
    st.write(", ".join(company_data['hashtags']) if isinstance(company_data['hashtags'], list) else "Aucun hashtag disponible.")

# Produits et secteurs
st.markdown("### Produits et secteurs :")
st.write("**Produits :**", ", ".join(company_data['product_types']) if isinstance(company_data['product_types'], list) else "Non spécifié")
st.write("**Secteurs :**", ", ".join(company_data['sectors']) if isinstance(company_data['sectors'], list) else "Non spécifié")

# Financements
st.markdown("### Informations supplémentaires :")
st.write(f"**Financement total :** {company_data['total_funding'] if pd.notna(company_data['total_funding']) else 'Non spécifié'}")
