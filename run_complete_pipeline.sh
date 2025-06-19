#!/bin/bash

echo "🚀 Ejecutando pipeline completo de WebLoader..."

# 0. Verificar configuración de base de datos
echo "📊 Paso 0: Verificando configuración de base de datos..."
if ! docker ps | grep -q "mysql"; then
    echo "❌ El contenedor MySQL no está ejecutándose"
    echo "💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d"
    exit 1
fi

# Verificar conexión a la base de datos
if ! docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;" >/dev/null 2>&1; then
    echo "❌ No se puede conectar a la base de datos proyecto02 con usuario 'pr'"
    echo "💡 Verifica que la base de datos y el usuario estén creados correctamente"
    exit 1
fi

echo "✅ Base de datos verificada correctamente"

# 1. Ejecutar Web Crawler
echo ""
echo "🕷️ Paso 1: Ejecutando Web Crawler..."
cd webCrawler
if ! python crawler.py; then
    echo "❌ Error ejecutando el web crawler"
    cd ..
    exit 1
fi
cd ..

# 2. Ejecutar análisis de Spark
echo ""
echo "⚡ Paso 2: Ejecutando análisis de Spark..."
chmod +x scripts/run_spark_analysis.sh
if ! ./scripts/run_spark_analysis.sh; then
    echo "❌ Error ejecutando análisis de Spark"
    exit 1
fi

echo ""
echo "✅ Pipeline de datos completado exitosamente"
echo "📊 Los datos han sido procesados y están disponibles en MySQL"

# 3. Iniciar aplicación web
echo ""
echo "🌐 Paso 3: Iniciando aplicación web..."
echo "💡 La aplicación estará disponible en: http://localhost:5000"
echo "⏹️ Presiona Ctrl+C para detener el servidor"
echo ""
cd WebLoader
python app.py

echo "✅ Pipeline completo ejecutado"