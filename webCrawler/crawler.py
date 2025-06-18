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
from datetime import datetime, timedelta
import subprocess
import os

container_name = "namenode"
local_folder = "wiki_data"
container_folder = "/wiki_data"
hdfs_target_dir = "/user/root/wiki_data"


stop_words = set(stopwords.words('spanish')) # Carga palabras vacías en español
visited = set()
base_url = "https://es.wikipedia.org"
output_dir = "wiki_data" # Directorio de salida
max_pages = 5 # Maximo de paginas a visitar
max_depth = 1 # Profundidad máxima del crawler

# Crear directorio si no existe
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, "wiki_data.jsonl")

def run_command(cmd):
    try:
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

# Limpiar texto y extrae palabras relevantes
def clean_text(text):
    text = unidecode(text.lower())
    words = word_tokenize(text)
    words = [w for w in words if w.isalpha() and w not in stop_words]
    return words

# Calcular promedio de ediciones por día desde la creación de la página
def extract_edits_per_day(soup, url):
    try:
        # Obtener el enlace al historial de la página
        history_link = soup.select_one('a[href*="action=history"]')
        
        if not history_link:
            return 0
        
        # Construir URL del historial
        history_url = urljoin(base_url, history_link['href'])
        
        # Realizar solicitud al historial
        history_response = requests.get(history_url, timeout=10)
        history_soup = BeautifulSoup(history_response.text, 'html.parser')
        
        # Buscar todas las ediciones en el historial
        edit_rows = history_soup.select('ul#pagehistory li, .mw-history-line, li[data-mw-revid]')
        
        if not edit_rows:
            return 0
        
        # Mapeo de meses en español
        months = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
                  'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}
        
        total_edits = 0
        creation_date = None
        latest_date = None
        
        for row in edit_rows:
            try:
                row_text = row.get_text()
                # Buscar patrón de fecha: "HH:MM DD/MM/YYYY"
                match = re.search(r'(\d{1,2}):(\d{2})\s+(\d{1,2})\s+(\w+)\s+(\d{4})', row_text)
                
                if match:
                    day, month_name, year = int(match.group(3)), match.group(4).lower(), int(match.group(5))
                    month = months.get(month_name, 0)
                    
                    if month > 0:
                        edit_date = datetime(year, month, day).date()
                        total_edits += 1
                        
                        # Actualizar fechas más temprana y más tardía
                        if creation_date is None or edit_date < creation_date:
                            creation_date = edit_date
                        if latest_date is None or edit_date > latest_date:
                            latest_date = edit_date
                            
            except (ValueError, IndexError):
                continue
        
        # Calcular promedio de ediciones por día
        if total_edits > 0 and creation_date and latest_date:
            days_span = (latest_date - creation_date).days + 1  # +1 para incluir el día de creación
            if days_span > 0:
                avg_edits_per_day = total_edits / days_span
                return round(avg_edits_per_day, 4)
        
        return 0
        
    except Exception:
        return 0


def upload_to_hdfs():
    if not os.path.exists(local_folder):
        print(f"Folder '{local_folder}' not found.")
        return

    
    print(f"Uploading file '{local_folder}'...")
    run_command(["docker", "cp", local_folder, f"{container_name}:{container_folder}"])

    print(f" Creating HDFS directory '{hdfs_target_dir}'...")
    run_command(["docker", "exec", container_name, "hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir])

    print(f"Uploading files from container to HDFS...")
    run_command(["docker", "exec", container_name, "hdfs", "dfs", "-put", "-f", "/wiki_data/wiki_data.jsonl", hdfs_target_dir])

    print(f"Verifying files in HDFS:")
    run_command([
        "docker", "exec", container_name, "hdfs", "dfs", "-ls", hdfs_target_dir
    ])


# Func. principal del crawler
def parse_page(url, depth):
    if url in visited or depth > max_depth or len(visited) >= max_pages:
        return

    # Normalizar URL
    visited.add(url)
    print(f"[{len(visited)}] Visitando: {url}")

    try:
        # Realizar solicitud HTTP y parsear HTML
        res = requests.get(url, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Extraer título y párrafos
        title = soup.find('h1').text if soup.find('h1') else "Sin título"
        paragraphs = soup.find_all('p')
        raw_text = ' '.join(p.get_text() for p in paragraphs)

        # Extraer palabras, bigramas y trigramas
        words = clean_text(raw_text)
        bigrams = [' '.join(b) for b in ngrams(words, 2)]
        trigrams = [' '.join(t) for t in ngrams(words, 3)]

        # Extraer enlaces relevantes
        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/wiki/') and ':' not in href:
                full_link = urljoin(base_url, href)
                links.add(full_link)

        # Diccionario con datos extraídos
        data = {
            'url': url,
            'title': title,
            'word_list': words,
            'bigrams': bigrams,
            'trigrams': trigrams,
            'edits_per_day': extract_edits_per_day(soup, url),
            'links': list(links)
        }

        # Guardar datos en archivo JSONL
        with open(output_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')

        # Recursión para seguir enlaces encontrados
        for link in links:
            if len(visited) >= max_pages:
                break
            parse_page(link, depth + 1)

        # Subir datos a HDFS
        if len(visited) >= max_pages:
            print("Subiendo datos a HDFS...")
            upload_to_hdfs()

    # Manejo de errores
    except Exception as e:
        print(f"Error en {url}: {e}")

# Inicia el crawler con una página inicial
start_url = "https://es.wikipedia.org/wiki/Inteligencia_artificial"
parse_page(start_url, 0)
