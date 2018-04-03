# Homework 1 - Xiping Liu (xl2639)
# How to run this script: python3 homework_1.py

import requests
import json

api_key = "Ch9ImJS5m6TijkjscOnbJM26U9eypZHbLwdf2Es3"
bday_last_year = "2017-07-12"
url = "https://api.nasa.gov/planetary/apod?api_key=" + \
        api_key + "&date=" + bday_last_year

try:
    response = requests.get(url)
    if not response.status_code == 200:
        print("HTTP error", response.status_code)
    else:
        try:
            response_data = response.json()
            print(response_data["url"])
        except:
            print("Response not in valid JSON format")
except:
    print("Something went wrong with requests.get")
