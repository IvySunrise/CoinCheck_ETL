import requests
import json 

def public_get(url, params=None): #Fetching the data from the API 
    response = requests.get(url, params = params)
    data = response.json()
    return data

