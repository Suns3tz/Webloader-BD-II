import requests
from bs4 import BeautifulSoup
import json
import os
import re
from urllib.parse import urljoin
from collections import Counter
from nltk.corpus import stopwords
from nltk import word_tokenize
from nltk.util import ngrams
from unidecode import unidecode

stop_words = set(stopwords.words('spanish'))
visited = set()
base_url = "https://es.wikipedia.org"
output_dir = "wiki_data"
max_pages = 5000
max_depth = 2

os.makedirs(output_dir, exist_ok=True)

def clean_text(text):
    text = unidecode(text.lower())
    words = word_tokenize(text)
    words = [w for w in words if w.isalpha() and w not in stop_words]
    return words

def extract_edits_per_day(url):
    return 0  # Placeholder (se puede mejorar después)

def parse_page(url, depth):
    if url in visited or depth > max_depth or len(visited) >= max_pages:
        return

    visited.add(url)
    print(f"[{len(visited)}] Visitando: {url}")

    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        title = soup.find('h1').text if soup.find('h1') else "Sin título"
        paragraphs = soup.find_all('p')
        raw_text = ' '.join(p.get_text() for p in paragraphs)

        words = clean_text(raw_text)
        bigrams = [' '.join(b) for b in ngrams(words, 2)]
        trigrams = [' '.join(t) for t in ngrams(words, 3)]

        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/wiki/') and ':' not in href:
                full_link = urljoin(base_url, href)
                links.add(full_link)

        data = {
            'url': url,
            'title': title,
            'word_list': words,
            'bigrams': bigrams,
            'trigrams': trigrams,
            'edits_per_day': extract_edits_per_day(url),
            'links': list(links)
        }

        file_name = os.path.join(output_dir, f"{len(visited)}.json")
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        for link in links:
            if len(visited) >= max_pages:
                break
            parse_page(link, depth + 1)

    except Exception as e:
        print(f"Error en {url}: {e}")

# Inicia el crawler con una página inicial
start_url = "https://es.wikipedia.org/wiki/Inteligencia_artificial"
parse_page(start_url, 0)