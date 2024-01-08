import requests
from bs4 import BeautifulSoup

url = ''
res = requests.get(url)

if res.ok:
    soup = BeautifulSoup(res.text, 'lxml')
    title = soup.find('title')

