#!/bin/bash

echo "🚀 Iniciando análisis de Spark..."

# Verificar si el contenedor de Spark está ejecutándose
if ! docker ps | grep -q "spark"; then
    echo "❌ El contenedor de Spark no está ejecutándose"
    echo "💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d"
    exit 1
fi

# Copiar el script de análisis al contenedor de Spark
echo "📂 Copiando script de análisis..."
docker cp spark_analyzer/spark_analyzer.py spark:/app/

# Ejecutar el análisis en el contenedor de Spark
echo "⚡ Ejecutando análisis..."
docker exec spark spark-submit \
    --packages mysql:mysql-connector-java:8.0.33 \
    /app/spark_analyzer.py

echo "✅ Análisis completado"