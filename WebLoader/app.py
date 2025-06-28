print("🚀 Iniciando WebLoader - Análisis de Páginas Web")

try:
    from flask import Flask, render_template, jsonify, request
    print("✅ Flask importado correctamente")
except ImportError as e:
    print(f"❌ Error importando Flask: {e}")
    exit(1)

try:
    import docker
    print("✅ Docker importado correctamente")
except ImportError as e:
    print(f"❌ Error importando Docker: {e}")
    print("⚠️ Continuando sin Docker...")
    docker = None

from datetime import datetime
import json
import subprocess
import os

# Importar API de análisis
from analysis_api import AnalysisAPI

app = Flask(__name__)

class DockerService:
    def __init__(self):
        if docker is None:
            print("⚠️ Docker module no disponible")
            self.client = None
            self.is_available = False
            return
            
        try:
            self.client = docker.from_env()
            # Test connection
            self.client.ping()
            self.is_available = True
            print("✅ Docker conectado correctamente")
        except Exception as e:
            print(f"⚠️ Docker no disponible: {e}")
            self.client = None
            self.is_available = False
    
    def get_container_status(self, container_name):
        """Obtiene el estado de un contenedor específico"""
        if not self.is_available:
            return {'name': container_name, 'status': 'docker_unavailable', 'running': False}
        
        try:
            container = self.client.containers.get(container_name)
            return {
                'name': container_name,
                'status': container.status,
                'running': container.status == "running",
                'id': container.short_id
            }
        except Exception:
            return {'name': container_name, 'status': 'not_found', 'running': False}
    
    def get_services_status(self):
        """Obtiene el estado de los servicios principales del proyecto"""
        services = ['mysql', 'namenode', 'spark', 'datanode', 'resourcemanager']
        status = {}
        
        for service in services:
            status[service] = self.get_container_status(service)
        
        return status

# Instancias globales
print("🐳 Inicializando servicios...")
docker_service = DockerService()
analysis_api = AnalysisAPI()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/docker/status')
def docker_status():
    """API endpoint para obtener estado de contenedores Docker"""
    return jsonify({
        'docker_available': docker_service.is_available,
        'services': docker_service.get_services_status(),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/analysis/submit', methods=['POST'])
def submit_analysis():
    """API endpoint para ejecutar análisis de Spark"""
    data = request.get_json()
    
    # Validación básica
    if not data or 'analysis_type' not in data:
        return jsonify({'error': 'Tipo de análisis requerido'}), 400
    
    try:
        # Obtener el directorio padre de WebLoader
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        
        # Ejecutar análisis de Spark
        if os.name == 'nt':  # Windows
            script_path = os.path.join(parent_dir, 'scripts', 'run_spark_analysis.bat')
        else:  # Linux/Mac
            script_path = os.path.join(parent_dir, 'scripts', 'run_spark_analysis.sh')
        
        # Verificar que el script existe
        if not os.path.exists(script_path):
            return jsonify({'error': f'Script no encontrado: {script_path}'}), 500
        
        # Ejecutar script en segundo plano desde el directorio padre
        process = subprocess.Popen([script_path], shell=True, cwd=parent_dir)
        
        return jsonify({
            'message': 'Análisis de Spark iniciado correctamente',
            'analysis_id': f"spark_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'status': 'running',
            'process_id': process.pid,
            'script_path': script_path
        })
        
    except Exception as e:
        return jsonify({'error': f'Error ejecutando análisis: {str(e)}'}), 500

@app.route('/api/results/summary')
def get_analysis_summary():
    """Obtener resumen del análisis"""
    summary = analysis_api.get_analysis_summary()
    
    if summary is None:
        return jsonify({'error': 'Error obteniendo resultados. Verifica que el análisis de Spark se haya ejecutado correctamente.'}), 500
    
    return jsonify(summary)

@app.route('/api/test')
def test():
    """Endpoint de prueba"""
    return jsonify({'status': 'ok', 'message': 'WebLoader API funcionando correctamente'})

@app.route('/api/database/test')
def test_database():
    """Endpoint para probar conexión a base de datos"""
    try:
        summary = analysis_api.get_analysis_summary()
        if summary is not None:
            return jsonify({
                'status': 'success', 
                'message': 'Conexión a MySQL exitosa',
                'data': summary
            })
        else:
            return jsonify({
                'status': 'error', 
                'message': 'No se pudo conectar a MySQL o no hay datos'
            }), 500
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': f'Error probando base de datos: {str(e)}'
        }), 500

# Nuevas rutas usando Stored Procedures

@app.route('/api/analysis/word/<word>')
def get_pages_by_word(word):
    """Obtener páginas que más copias de una palabra tienen"""
    results = analysis_api.get_top_pages_by_word(word)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/word-set2/<word1>/<word2>')
def get_pages_by_word_set2(word1, word2):
    """Obtener páginas que más copias de un set de 2 palabras tienen"""
    results = analysis_api.get_top_pages_by_word_set2(word1, word2)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/word-set3/<word1>/<word2>/<word3>')
def get_pages_by_word_set3(word1, word2, word3):
    """Obtener páginas que más copias de un set de 3 palabras tienen"""
    results = analysis_api.get_top_pages_by_word_set3(word1, word2, word3)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/shared-bigrams')
def get_shared_bigrams():
    """Obtener páginas con más sets de 2 palabras coincidentes con una página dada"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_shared_bigrams_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/shared-trigrams')
def get_shared_trigrams():
    """Obtener páginas con más sets de 3 palabras coincidentes con una página dada"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_shared_trigrams_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/page-words')
def get_page_words():
    """Obtener el set de palabras distintas de una página y su cantidad"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_different_words_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/page-links')
def get_page_links():
    """Obtener la cantidad de links distintos que aparecen en una página"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_link_count_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/page-word-percentages')
def get_page_word_percentages():
    """Obtener el porcentaje que representa cada palabra en el texto total de la página"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_percentage_words_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/word-repetitions/<word>')
def get_word_repetitions(word):
    """Obtener la cantidad total de repeticiones de una palabra"""
    results = analysis_api.get_total_repetitions_by_word(word)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/analysis/page-repetitions')
def get_page_repetitions():
    """Obtener la cantidad total de repeticiones de una página"""
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Parámetro URL requerido'}), 400
    
    results = analysis_api.get_total_repetitions_by_page(url)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/helper/pages')
def get_available_pages():
    """Obtener lista de páginas disponibles para facilitar las pruebas"""
    limit = request.args.get('limit', 50, type=int)
    results = analysis_api.get_available_pages(limit)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

@app.route('/api/helper/words')
def get_available_words():
    """Obtener lista de palabras disponibles para facilitar las pruebas"""
    limit = request.args.get('limit', 100, type=int)
    results = analysis_api.get_available_words(limit)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

# Análisis #9: Endpoint para tópicos más interconectados por enlaces
@app.route('/api/analysis/topic-connections')
def get_topic_connections():
    """Obtener los tópicos más interconectados basado en el grafo de enlaces"""
    limit = request.args.get('limit', 20, type=int)
    results = analysis_api.get_most_interconnected_topics(limit)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({'success': True, 'data': results})

if __name__ == '__main__':
    print("📍 Servidor disponible en: http://localhost:5000")
    print("🐳 Estado Docker:", "✅ Conectado" if docker_service.is_available else "❌ No disponible")
    print("💾 Probando conexión a MySQL...")
    
    # Probar conexión a base de datos al inicio
    test_summary = analysis_api.get_analysis_summary()
    if test_summary is not None:
        print("✅ Conexión a MySQL exitosa")
        print(f"📊 Datos disponibles: {test_summary}")
    else:
        print("⚠️ No se pudo conectar a MySQL o no hay datos disponibles")
        print("💡 Ejecuta el análisis de Spark primero")
    
    print("⏹️ Presiona Ctrl+C para detener")
    print("-" * 50)
    
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        print(f"❌ Error iniciando servidor: {e}")
        print("💡 Asegúrate de que el puerto 5000 esté libre")
