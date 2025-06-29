import requests
from bs4 import BeautifulSoup
import re
import json
import time
from urllib.parse import urljoin, unquote, quote
from nltk.util import ngrams
from nltk.corpus import stopwords
import nltk
import threading
from queue import Queue
from threading import Semaphore
import os
import subprocess
from datetime import datetime

# Base configuration
BASE_WIKI = "https://es.wikipedia.org/wiki/"
REST_API_HTML = "https://es.wikipedia.org/api/rest_v1/page/html/"
API_URL = "https://es.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "WikipediaCrawlerBot/1.0 (bliang@estudiantec.cr)"}

MAX_DEPTH = 3
MAX_DATA_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_THREADS = 50  # Número de threads concurrentes (dentro del límite de rate)
REQUESTS_PER_SECOND = 200  # Límite de la API
REQUEST_INTERVAL = 1.0 / REQUESTS_PER_SECOND  # Intervalo entre requests

# HDFS Configuration
CONTAINER_NAME = "namenode"
LOCAL_FOLDER = "wiki_data"
CONTAINER_FOLDER = "/wiki_data"
HDFS_TARGET_DIR = "/user/root/wiki_data"

# Cache file configuration
CACHE_FILE = "visited_pages_cache.json"

# Descargar stopwords de NLTK si no están disponibles
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

EXCLUDED_WORDS = set(stopwords.words('english'))

# Variables compartidas y mecanismos de control
visited = set()
visited_lock = threading.Lock()
total_data_size = 0
size_lock = threading.Lock()
queue = Queue()
output_lock = threading.Lock()
last_request_time = 0
rate_lock = threading.Lock()
request_semaphore = Semaphore(REQUESTS_PER_SECOND)  # Control de rate
cache_lock = threading.Lock()  # Lock para acceso al cache

def run_command(cmd):
    """Execute subprocess command with error handling"""
    try:
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False

def setup_hdfs_environment():
    """Setup local folder and HDFS environment"""
    # Create local directory if it doesn't exist
    os.makedirs(LOCAL_FOLDER, exist_ok=True)
    print(f"Local folder '{LOCAL_FOLDER}' ready")
    
    # Create HDFS directory
    print(f"Creating HDFS directory '{HDFS_TARGET_DIR}'...")
    return run_command([
        "docker", "exec", CONTAINER_NAME, 
        "hdfs", "dfs", "-mkdir", "-p", HDFS_TARGET_DIR
    ])

def upload_to_hdfs():
    """Upload files to HDFS"""
    if not os.path.exists(LOCAL_FOLDER):
        print(f"Folder '{LOCAL_FOLDER}' not found.")
        return False

    print(f"Uploading folder '{LOCAL_FOLDER}' to container...")
    if not run_command(["docker", "cp", LOCAL_FOLDER, f"{CONTAINER_NAME}:{CONTAINER_FOLDER}"]):
        return False

    print(f"Uploading files from container to HDFS...")
    if not run_command([
        "docker", "exec", CONTAINER_NAME, 
        "hdfs", "dfs", "-put", "-f", "/wiki_data/wiki_data.jsonl", HDFS_TARGET_DIR
    ]):
        return False

    print(f"Verifying files in HDFS:")
    return run_command([
        "docker", "exec", CONTAINER_NAME, 
        "hdfs", "dfs", "-ls", HDFS_TARGET_DIR
    ])

def check_hdfs_status():
    """Check HDFS status and available space"""
    print("Checking HDFS status...")
    run_command([
        "docker", "exec", CONTAINER_NAME, 
        "hdfs", "dfsadmin", "-report"
    ])

def load_visited_cache():
    """Carga las páginas visitadas desde el archivo de cache"""
    global visited
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                visited = set(cache_data.get('visited_pages', []))
                print(f"Loaded {len(visited)} previously visited pages from cache")
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading cache file: {e}")
            visited = set()
    else:
        visited = set()
        print("No cache file found, starting fresh")

def save_visited_cache():
    """Guarda las páginas visitadas al archivo de cache"""
    try:
        with cache_lock:
            cache_data = {
                'visited_pages': list(visited),
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except IOError as e:
        print(f"Error saving cache file: {e}")

def add_to_visited_cache(page_id):
    """Añade una página al cache de visitadas y actualiza el archivo"""
    with visited_lock:
        if page_id not in visited:
            visited.add(page_id)
            # Actualizar el cache cada 10 páginas para no sobrecargar el I/O
            if len(visited) % 10 == 0:
                threading.Thread(target=save_visited_cache, daemon=True).start()
            return True
    return False

def is_page_visited(page_id):
    """Verifica si una página ya ha sido visitada"""
    with visited_lock:
        return page_id in visited

class RateLimiter:
    def __init__(self, rate):
        self.rate = rate
        self.tokens = rate
        self.last_check = time.time()
        self.lock = threading.Lock()

    def consume(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_check
            self.last_check = now
            
            # Añadir tokens basados en el tiempo transcurrido
            self.tokens += elapsed * self.rate
            if self.tokens > self.rate:
                self.tokens = self.rate
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# Global rate limiter
rate_limiter = RateLimiter(REQUESTS_PER_SECOND)

def clean_and_process_text(text):
    text = re.sub(r'[^\w\s]', '', text, flags=re.UNICODE).lower()
    words = text.split()
    return [word for word in words if word not in EXCLUDED_WORDS]

def generate_ngrams(words, n):
    return [' '.join(gram) for gram in ngrams(words, n)] if len(words) >= n else []

def get_page_html_rest(title):
    global last_request_time
    
    # Esperar nuestro turno para cumplir con el rate limit
    while not rate_limiter.consume():
        time.sleep(0.001)  # Espera activa muy corta
    
    url = REST_API_HTML + quote(title.replace(' ', '_'))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.text
        else:
            print(f"[{resp.status_code}] Failed to fetch {title}")
            return None
    except requests.RequestException as e:
        print(f"Request error fetching {title}: {str(e)}")
        return None

def extract_links(soup):
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/wiki/'):
            if ':' not in href and '#' not in href:
                full_url = urljoin(BASE_WIKI, href)
                links.add(full_url)
        elif href.startswith('./'):
            clean_href = href[2:]
            if clean_href and ':' not in clean_href and '#' not in clean_href:
                full_url = BASE_WIKI + clean_href
                links.add(full_url)
        elif 'wikipedia.org/wiki/' in href:
            if ':' not in href.split('/wiki/')[-1] and '#' not in href:
                links.add(href)
    
    return list(links)

def get_edit_rate(title):
    # También aplicamos rate limiting a las llamadas a la API
    while not rate_limiter.consume():
        time.sleep(0.001)
    
    try:
        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "rvlimit": "500",
            "rvprop": "timestamp",
            "titles": title
        }
        resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=10)
        data = resp.json()
        pages = data.get('query', {}).get('pages', {})
        for page in pages.values():
            revisions = page.get('revisions', [])
            if len(revisions) < 2:
                return 0
            timestamps = [time.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%SZ") for r in revisions]
            timestamps.sort()
            start = time.mktime(timestamps[0])
            end = time.mktime(timestamps[-1])
            days = max((end - start) / (60 * 60 * 24), 1)
            return round(len(revisions) / days, 2)
    except Exception as e:
        print(f"Error getting edit rate for {title}: {str(e)}")
        return 0

def process_page(title, depth, file_handle):
    global total_data_size
    
    # Verificar si ya visitamos esta página
    if is_page_visited(title):
        print(f"Skipping already visited page: {title}")
        return
    
    html = get_page_html_rest(title)
    if not html:
        return

    # Marcar como visitada después de obtener el HTML exitosamente
    add_to_visited_cache(title)

    soup = BeautifulSoup(html, "html.parser")
    text_blocks = soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5'])
    full_text = " ".join(block.get_text(separator=" ", strip=True) for block in text_blocks)

    page_title_tag = soup.find('h1')
    page_title = page_title_tag.text.strip() if page_title_tag else title.replace('_', ' ')

    word_list = clean_and_process_text(full_text)
    bigrams = generate_ngrams(word_list, 2)
    trigrams = generate_ngrams(word_list, 3)
    edits_per_day = get_edit_rate(page_title)
    links = extract_links(soup)

    item = {
        "url": BASE_WIKI + quote(title.replace(' ', '_')),
        "title": page_title,
        "word_list": word_list,
        "bigrams": bigrams,
        "trigrams": trigrams,
        "edits_per_day": edits_per_day,
        "links": links
    }

    json_line = json.dumps(item, ensure_ascii=False)
    
    with output_lock:
        file_handle.write(json_line + "\n")
        file_handle.flush()
    
    with size_lock:
        total_data_size += len(json_line.encode('utf-8'))
        if total_data_size >= MAX_DATA_SIZE:
            # Vaciar la cola si alcanzamos el límite de tamaño
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except:
                    pass
    
    print(f"Crawled: {page_title} | Depth: {depth} | Links: {len(links)} | Size: {total_data_size / (1024 * 1024):.2f} MB")

    # Añadir nuevos links a la cola si no hemos alcanzado el límite
    if depth < MAX_DEPTH and total_data_size < MAX_DATA_SIZE:
        for link_url in links:
            link_title = unquote(link_url.split("/wiki/")[-1])
            if not is_page_visited(link_title):
                queue.put((link_title, depth + 1))

def worker(file_handle):
    while True:
        try:
            title, depth = queue.get(timeout=5)  # Timeout más corto para responder más rápido al cierre
            
            if total_data_size >= MAX_DATA_SIZE:
                queue.task_done()
                break
                
            process_page(title, depth, file_handle)
            queue.task_done()
        except Queue.Empty:
            break
        except Exception as e:
            print(f"Error in worker thread: {str(e)}")
            queue.task_done()

def crawl_rest(start_title, file_handle):
    global total_data_size
    
    # Cargar cache de páginas visitadas al inicio
    load_visited_cache()
    
    # Solo añadir la página inicial si no ha sido visitada
    if not is_page_visited(start_title):
        queue.put((start_title, 0))
    else:
        print(f"Start page {start_title} already visited, loading from cache")
    
    # Crear y lanzar threads
    threads = []
    for _ in range(MAX_THREADS):
        t = threading.Thread(target=worker, args=(file_handle,))
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Esperar a que se complete la cola
    queue.join()
    
    # Esperar a que todos los threads terminen
    for t in threads:
        t.join()
    
    # Guardar el cache final
    save_visited_cache()

def main():
    """Main function with HDFS integration"""
    # Setup HDFS environment
    print("Setting up HDFS environment...")
    if not setup_hdfs_environment():
        print("Failed to setup HDFS environment. Continuing with local storage only.")
    
    # Check HDFS status
    check_hdfs_status()
    
    # Create output file in the local wiki_data folder
    output_file = os.path.join(LOCAL_FOLDER, "wiki_data.jsonl")
    
    with open(output_file, "w", encoding="utf-8") as f:
        start_time = time.time()
        try:
            crawl_rest("Inteligencia_artificial", f)
        except KeyboardInterrupt:
            print("\nReceived keyboard interrupt. Shutting down gracefully...")
            # Guardar el cache antes de salir
            save_visited_cache()
            # Vaciar la cola para permitir que los threads terminen
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except:
                    pass
    
    total_time = time.time() - start_time
    total_requests = len(visited)
    
    print(f"\n✅ Finished crawling. Total data: {total_data_size / (1024 * 1024):.2f} MB")
    print(f"Time taken: {total_time:.2f} seconds")
    print(f"Total pages crawled: {total_requests}")
    print(f"Average request rate: {total_requests/max(total_time, 1):.2f} requests/sec")
    print(f"Cache saved to: {CACHE_FILE}")
    print(f"Output file: {output_file}")
    

if __name__ == "__main__":
    main()