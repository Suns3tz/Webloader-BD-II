import docker
import requests
import mysql.connector
from mysql.connector import Error
import time
import subprocess

def check_container_status(container_name):
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        if container.status == "running":
            print(f"✅ {container_name} está corriendo (Estado: {container.status})")
            return True
        else:
            print(f"❌ {container_name} no está corriendo (Estado: {container.status})")
            return False
    except Exception as e:
        print(f"⚠️ Error al verificar {container_name}: {str(e)}")
        return False

def test_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='user',
            password='password',
            database='testdb'
        )
        if connection.is_connected():
            print("✅ Conexión a MySQL exitosa")
            connection.close()
            return True
    except Error as e:
        print(f"❌ Error de conexión a MySQL: {e}")
        return False

def test_hadoop_connection():
    attempts = 3
    for i in range(attempts):
        try:
            response = requests.get('http://localhost:9870', timeout=10)
            if response.status_code == 200:
                print("✅ Interfaz web de Hadoop accesible")
                return True
            else:
                print(f"⚠️ Intento {i+1}/{attempts}: Hadoop respondió con código {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Intento {i+1}/{attempts}: Error de conexión ({type(e).__name__})")
        time.sleep(5)

    # Último recurso: verificar desde dentro de un contenedor Hadoop
    try:
        output = subprocess.check_output(
            ['docker', 'exec', 'namenode', 'curl', '-s', 'http://localhost:9870'],
            stderr=subprocess.STDOUT,
            timeout=15
        )
        print("ℹ️ Hadoop responde INTERNAMENTE pero no desde el host")
        return True
    except subprocess.SubprocessError:
        print("❌ Hadoop no responde ni internamente")
        return False

def test_spark_connection():
    try:
        response = requests.get('http://localhost:8080', timeout=5)
        if response.status_code == 200:
            print("✅ Interfaz web de Spark accesible")
            return True
        else:
            print(f"❌ Spark respondió con código {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ No se pudo acceder a Spark: {e}")
        return False

def main():
    print("\n🔍 Iniciando verificación de contenedores Docker...\n")
    
    # Definimos los contenedores por proyecto
    dockercontainer_services = {
        'mysql': test_mysql_connection,
        'spark': test_spark_connection
    }

    dockerhadoop_services = {
        'namenode': test_hadoop_connection,
        'resourcemanager': None,
        'nodemanager': None,
        'datanode': None,
        'historyserver': None
    }

    # Detectar cuáles están corriendo
    active_services = {}
    print("📦 Verificando contenedores de dockercontainer...")
    for name, test_func in dockercontainer_services.items():
        if check_container_status(name):
            active_services[name] = test_func

    print("\n📦 Verificando contenedores de docker-hadoop...")
    for name, test_func in dockerhadoop_services.items():
        if check_container_status(name):
            if test_func:
                active_services[name] = test_func

    if not active_services:
        print("\n⚠️ No se encontraron contenedores activos.")
        return

    # Ejecutar pruebas de conectividad
    print("\n🧪 Realizando pruebas de conectividad...\n")
    resultados = {}
    for name, test_func in active_services.items():
        if test_func:
            resultados[name] = test_func()
        else:
            resultados[name] = "No probado"

    # Mostrar resumen
    print("\n📊 Resumen de estado:")
    for name, estado in resultados.items():
        simbolo = '✅' if estado is True else ('❌' if estado is False else '⚠️')
        print(f"{name}: {simbolo}")

    # Sugerencia si Hadoop falla
    if resultados.get("namenode") is False:
        print("\n🔧 Solución sugerida para Hadoop:")
        print("1. Reconstruir contenedor: docker-compose up -d --force-recreate docker-hadoop")
        print("2. Ver logs: docker logs namenode")
        print("3. Verificar configuración: docker exec namenode cat /opt/hadoop/etc/hadoop/core-site.xml")

    print("\n🛑 Verificación completada\n")

if __name__ == "__main__":
    main()