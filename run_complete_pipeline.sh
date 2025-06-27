#!/bin/bash
set -e

# 🚀 Ejecutando pipeline completo de WebLoader desde cero...
echo "🚀 Ejecutando pipeline completo de WebLoader desde cero..."

echo "📋 Verificando requisitos del sistema..."

# Verificar que Docker está ejecutándose
docker version >/dev/null 2>&1 || { echo "❌ Docker no está ejecutándose"; echo "💡 Inicia Docker primero"; exit 1; }

# Verificar que docker-compose está disponible
docker-compose version >/dev/null 2>&1 || { echo "❌ Docker Compose no está disponible"; exit 1; }

echo "✅ Docker verificado correctamente"

echo
# === PASO 0: INICIAR CONTENEDORES CON DOCKER COMPOSE ===
echo "🐳 Paso 0: Iniciando contenedores con Docker Compose..."

echo "🛑 Deteniendo contenedores existentes..."
docker-compose -f docker/hadoop/docker-compose.yml down || true
docker-compose -f docker/spark-mysql/docker-compose.yml down || true

echo "🗂️ Iniciando Hadoop..."
docker-compose -f docker/hadoop/docker-compose.yml up -d || { echo "❌ Error iniciando Hadoop"; exit 1; }

echo "⏳ Esperando que Hadoop esté listo..."
sleep 10

echo "⚡ Iniciando Spark y MySQL..."
docker-compose -f docker/spark-mysql/docker-compose.yml up -d || { echo "❌ Error iniciando Spark y MySQL"; exit 1; }

echo "⏳ Esperando que todos los servicios estén listos..."
sleep 10

echo "🔍 Verificando estado de contenedores..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Verificar contenedores específicos
docker ps | grep namenode >/dev/null || { echo "❌ Namenode no está ejecutándose"; exit 1; }
docker ps | grep spark >/dev/null || { echo "❌ Spark no está ejecutándose"; exit 1; }
docker ps | grep mysql >/dev/null || { echo "❌ MySQL no está ejecutándose"; exit 1; }

echo "✅ Todos los contenedores están ejecutándose correctamente"

echo
# === PASO 1: CONFIGURACIÓN COMPLETA DE BASE DE DATOS ===
echo "🗄️ Paso 1: Configuración completa de base de datos MySQL..."

echo "⏳ Esperando que MySQL esté completamente listo..."
sleep 10

echo "🗑️ Eliminando base de datos anterior..."
docker exec mysql mysql -u root -proot -e "DROP DATABASE IF EXISTS proyecto02;" || true

echo "🔨 Creando base de datos y usuario..."
docker exec mysql mysql -u root -proot -e "CREATE DATABASE proyecto02;" || { echo "❌ Error creando base de datos"; exit 1; }
docker exec mysql mysql -u root -proot -e "CREATE USER IF NOT EXISTS 'pr'@'%' IDENTIFIED BY 'pr';"
docker exec mysql mysql -u root -proot -e "GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%';"
docker exec mysql mysql -u root -proot -e "FLUSH PRIVILEGES;"
docker exec mysql mysql -u root -proot -e "SET GLOBAL log_bin_trust_function_creators = 1;"

echo "📊 Creando tablas..."
docker cp scripts/02_TABLES_PK_COMMENTS.sql mysql:/tmp/
docker cp scripts/03_INDEXES.sql mysql:/tmp/
docker cp scripts/04_QUERIES.sql mysql:/tmp/

docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/02_TABLES_PK_COMMENTS.sql"
docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/03_INDEXES.sql"
docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/04_QUERIES.sql" || { echo "❌ Error creando tablas"; exit 1; }

echo "✅ Base de datos configurada correctamente"
echo "📊 Verificando tablas creadas:"
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"

echo
# === PASO 2: EJECUTAR WEB CRAWLER ===
echo "🕷️ Paso 2: Ejecutando Web Crawler..."
cd webCrawler
python3 crawler.py || { echo "❌ Error ejecutando el web crawler"; cd ..; exit 1; }
cd ..

echo "🔍 Verificando datos en HDFS..."
docker exec namenode hdfs dfs -test -e /user/root/wiki_data/wiki_data.jsonl || {
    echo "❌ No se encontró el archivo wiki_data.jsonl en HDFS"
    echo "💡 Verifica que el crawler haya subido los datos correctamente"
    echo "📂 Contenido actual de HDFS:"
    docker exec namenode hdfs dfs -ls /user/root/ || true
    docker exec namenode hdfs dfs -ls /user/root/wiki_data || true
    exit 1
}

echo "✅ Archivo wiki_data.jsonl encontrado en HDFS"
echo "📂 Información del archivo:"
docker exec namenode hdfs dfs -du -h /user/root/wiki_data

echo
# === PASO 3: EJECUTAR ANÁLISIS DE SPARK ===
echo "⚡ Paso 3: Ejecutando análisis de Spark..."

echo "📂 Copiando script de análisis..."
docker cp spark_analyzer/spark_analyzer.py spark:/app/ || { echo "❌ Error copiando script al contenedor Spark"; exit 1; }

echo "📦 Instalando dependencias en Spark..."
docker exec spark pip install mysql-connector-python || echo "⚠️ Warning: No se pudo instalar mysql-connector-python, continuando..."

echo "⚡ Ejecutando análisis..."
echo "📊 Procesando datos desde HDFS..."
echo "💾 Guardando resultados en MySQL..."
docker exec spark spark-submit --packages mysql:mysql-connector-java:8.0.33 /app/spark_analyzer.py || { echo "❌ Error ejecutando análisis de Spark"; exit 1; }

echo "✅ Análisis de Spark completado exitosamente"

echo
# === PASO 4: VERIFICACIÓN FINAL ===
echo "🔍 Paso 4: Verificación final del sistema..."

echo "📊 === ESTADO DE CONTENEDORES ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo

echo "💾 === DATOS EN MYSQL ==="
echo "🔍 Verificando datos insertados:"
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SELECT COUNT(*) as 'Páginas' FROM Page; SELECT COUNT(*) as 'Palabras' FROM Word; SELECT COUNT(*) as 'Relaciones Palabra-Página' FROM TopWordPages;"

echo

echo "📂 === ARCHIVOS EN HDFS ==="
docker exec namenode hdfs dfs -ls /user/root/wiki_data || {
    echo "❌ No se puede acceder a HDFS o no existen archivos"
} && {
    echo "✅ Archivos encontrados en HDFS"
    docker exec namenode hdfs dfs -du -h /user/root/wiki_data
}

echo

echo "🌐 === INTERFACES WEB DISPONIBLES ==="
echo "📊 Hadoop NameNode: http://localhost:9870"
echo "⚡ Spark Master: http://localhost:8080"
echo "🌐 WebLoader App: http://localhost:5000"

echo

echo "✅ Pipeline de datos completado exitosamente"
echo "📊 Los datos han sido procesados y están disponibles en MySQL"

echo
# === PASO 5: INICIAR APLICACIÓN WEB ===
echo "🌐 Paso 5: Iniciando aplicación web..."
echo "💡 La aplicación estará disponible en: http://localhost:5000"
echo "⏹️ Presiona Ctrl+C para detener el servidor"
echo

cd WebLoader
python3 app.py

echo

echo "✅ Pipeline completo ejecutado"
echo

echo "🛑 Para detener todos los servicios ejecuta:"
echo "   docker-compose -f docker/hadoop/docker-compose.yml down"
echo "   docker-compose -f docker/spark-mysql/docker-compose.yml down"
