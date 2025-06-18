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

# Instancia global del servicio Docker
print("🐳 Inicializando servicio Docker...")
docker_service = DockerService()

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
    """API endpoint para enviar análisis (placeholder para implementación futura)"""
    data = request.get_json()
    
    # Validación básica
    if not data or 'analysis_type' not in data:
        return jsonify({'error': 'Tipo de análisis requerido'}), 400
    
    # Placeholder para la lógica futura
    return jsonify({
        'message': 'Análisis enviado correctamente',
        'analysis_id': f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'status': 'pending',
        'data_received': data
    })

@app.route('/api/test')
def test():
    """Endpoint de prueba"""
    return jsonify({'status': 'ok', 'message': 'WebLoader API funcionando correctamente'})

if __name__ == '__main__':
    print("📍 Servidor disponible en: http://localhost:5000")
    print("🐳 Estado Docker:", "✅ Conectado" if docker_service.is_available else "❌ No disponible")
    print("⏹️ Presiona Ctrl+C para detener")
    print("-" * 50)
    
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        print(f"❌ Error iniciando servidor: {e}")
        print("💡 Asegúrate de que el puerto 5000 esté libre")
