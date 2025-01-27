import requests

url = "https://www.minalogic.com/annuaire/"
r = requests.get(url)
print(r.status_code)    # doit être 200
print(len(r.text))      # la longueur du HTML
print(r.text[:1000])    # un aperçu du contenu
