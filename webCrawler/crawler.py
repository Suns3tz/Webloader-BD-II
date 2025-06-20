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
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

container_name = "namenode"
local_folder = "wiki_data"
container_folder = "/wiki_data"
hdfs_target_dir = "/user/root/wiki_data"

stop_words = set(stopwords.words('spanish')) # carga palabras vacías en español
visited = set() 
visited_lock = threading.Lock()
base_url = "https://es.wikipedia.org"
output_dir = "wiki_data"
max_pages = 5 # Número máximo de páginas a visitar
max_depth = 5 # Profundidad máxima de enlaces a seguir
max_workers = 8  # Número de hilos

# Crear directorio si no existe
os.makedirs(output_dir, exist_ok=True)
output_file_path = os.path.join(output_dir, "wiki_data.jsonl")
output_file_lock = threading.Lock() 

# Cola para URLs pendientes
url_queue = queue.Queue()

def run_command(cmd):
    try:
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def clean_text(text):
    text = unidecode(text.lower())
    words = word_tokenize(text)
    words = [w for w in words if w.isalpha() and w not in stop_words]
    return words

def extract_edits_per_day(soup, url):
    try:
        history_link = soup.select_one('a[href*="action=history"]')
        
        if not history_link:
            return 0
        
        history_url = urljoin(base_url, history_link['href'])
        history_response = requests.get(history_url, timeout=10)
        history_soup = BeautifulSoup(history_response.text, 'html.parser')
        
        edit_rows = history_soup.select('ul#pagehistory li, .mw-history-line, li[data-mw-revid]')
        
        if not edit_rows:
            return 0
        
        months = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
                  'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}
        
        total_edits = 0
        creation_date = None
        latest_date = None
        
        for row in edit_rows:
            try:
                row_text = row.get_text()
                match = re.search(r'(\d{1,2}):(\d{2})\s+(\d{1,2})\s+(\w+)\s+(\d{4})', row_text)
                
                if match:
                    day, month_name, year = int(match.group(3)), match.group(4).lower(), int(match.group(5))
                    month = months.get(month_name, 0)
                    
                    if month > 0:
                        edit_date = datetime(year, month, day).date()
                        total_edits += 1
                        
                        if creation_date is None or edit_date < creation_date:
                            creation_date = edit_date
                        if latest_date is None or edit_date > latest_date:
                            latest_date = edit_date
                            
            except (ValueError, IndexError):
                continue
        
        if total_edits > 0 and creation_date and latest_date:
            days_span = (latest_date - creation_date).days + 1
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

def is_crawling_complete():
    """Verifica si el crawling debe detenerse"""
    with visited_lock:
        return len(visited) >= max_pages

def add_urls_to_queue(links, current_depth):
    """Añade URLs a la cola si no han sido visitadas"""
    if current_depth >= max_depth:
        return
    
    for link in links:
        with visited_lock:
            if link not in visited and len(visited) < max_pages:
                url_queue.put((link, current_depth + 1))

def save_data_to_file(data):
    """Guarda datos en archivo JSONL de forma thread-safe"""
    with output_file_lock:
        with open(output_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')

def parse_page_worker(url, depth):
    """Worker function que procesa una sola página"""
    # Verificar si ya fue visitada
    with visited_lock:
        if url in visited or len(visited) >= max_pages:
            return None
        visited.add(url)
        current_count = len(visited)
    
    print(f"[{current_count}] Visitando: {url} (Profundidad: {depth})")

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

        # Guardar datos en archivo
        save_data_to_file(data)

        # Añadir nuevos enlaces a la cola
        add_urls_to_queue(links, depth)

        return data

    except Exception as e:
        print(f"Error en {url}: {e}")
        return None

def crawler_worker():
    """Función worker que procesa URLs de la cola"""
    while not is_crawling_complete():
        try:
            # Obtener URL de la cola con timeout
            url, depth = url_queue.get(timeout=5)
            
            # Procesar la página
            parse_page_worker(url, depth)
            
            # Marcar tarea como completada
            url_queue.task_done()
            
        except queue.Empty:
            # Si no hay URLs en la cola, continuar
            continue
        except Exception as e:
            print(f"Error en worker: {e}")

def run_threaded_crawler():
    """Ejecuta el crawler con múltiples hilos"""
    start_url = "https://es.wikipedia.org/wiki/Inteligencia_artificial"
    
    # Añadir URL inicial a la cola
    url_queue.put((start_url, 0))
    
    # Crear y ejecutar hilos
    threads = []
    for i in range(max_workers):
        t = threading.Thread(target=crawler_worker, daemon=True)
        t.start()
        threads.append(t)
        print(f"Iniciado hilo {i+1}/{max_workers}")
    
    # Monitorear progreso
    start_time = time.time()
    while not is_crawling_complete():
        time.sleep(10)  # Revisar cada 10 segundos
        with visited_lock:
            current_count = len(visited)
        
        elapsed_time = time.time() - start_time
        print(f"Progreso: {current_count}/{max_pages} páginas procesadas en {elapsed_time:.1f} segundos")
        
        # Si la cola está vacía y no se han alcanzado las páginas máximas, esperar un poco más
        if url_queue.empty() and current_count < max_pages:
            print("Cola vacía, esperando...")
            time.sleep(5)
    
    print("Crawling completado. Esperando que terminen los hilos...")
    
    # Esperar a que terminen todos los hilos
    for t in threads:
        t.join(timeout=30)
    
    print(f"Crawler finalizado. Total de páginas visitadas: {len(visited)}")
    
    # Subir datos a HDFS
    print("Completado...")

# Ejecutar el crawler
if __name__ == "__main__":
    print("Que desea realizar:\n"+
          "1. Crawler\n"+
          "2. Subir a HDFS\n"
          )
    choice = input("> ").strip()
    if choice == "1":
        run_threaded_crawler()
    elif choice == "2":
        upload_to_hdfs()
    else:
        print("Opción no válida. Saliendo...")
