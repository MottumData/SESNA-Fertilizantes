import requests
from bs4 import BeautifulSoup
import os   
import pandas as pd

def scrape_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    urls = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http') and href.endswith('.csv'):
            urls.append(href)

    return urls

def scrape_xlsx(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    urls = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http') and href.endswith('.xlsx'):
            urls.append(href)

    return urls

'''
url = "https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-autorizados"
count = 0
urls = scrape_urls(url)
for url in urls:
    print(url)
    print(count)
'''