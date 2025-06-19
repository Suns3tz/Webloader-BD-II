@echo off
echo 🚀 Iniciando análisis de Spark...

REM Verificar si el contenedor de Spark está ejecutándose
docker ps | findstr "spark" >nul
if errorlevel 1 (
    echo ❌ El contenedor de Spark no está ejecutándose
    echo 💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d
    exit /b 1
)

REM Verificar si el contenedor de MySQL está ejecutándose
docker ps | findstr "mysql" >nul
if errorlevel 1 (
    echo ❌ El contenedor de MySQL no está ejecutándose
    echo 💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d
    exit /b 1
)

REM Copiar el script de análisis al contenedor de Spark
echo 📂 Copiando script de análisis...
docker cp spark_analyzer\spark_analyzer.py spark:/app/
if errorlevel 1 (
    echo ❌ Error copiando script al contenedor Spark
    exit /b 1
)

REM Instalar mysql-connector-python en el contenedor
echo 📦 Instalando dependencias en Spark...
docker exec spark pip install mysql-connector-python
if errorlevel 1 (
    echo ⚠️ Warning: No se pudo instalar mysql-connector-python, continuando...
)

REM Ejecutar el análisis en el contenedor de Spark
echo ⚡ Ejecutando análisis...
echo 📊 Procesando datos desde HDFS...
echo 💾 Guardando resultados en MySQL...
docker exec spark spark-submit --packages mysql:mysql-connector-java:8.0.33 /app/spark_analyzer.py
if errorlevel 1 (
    echo ❌ Error ejecutando análisis de Spark
    exit /b 1
)

echo ✅ Análisis completado exitosamente
echo 📊 Los datos están ahora disponibles en las tablas de MySQL