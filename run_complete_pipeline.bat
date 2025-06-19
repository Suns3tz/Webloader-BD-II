@echo off
echo 🚀 Ejecutando pipeline completo de WebLoader desde cero...

REM 0. Reiniciar base de datos completamente
echo 📊 Paso 0: Reiniciando base de datos desde cero...
call scripts\reset_complete_database.bat
if errorlevel 1 (
    echo ❌ Error reiniciando la base de datos
    pause
    exit /b 1
)

REM 1. Ejecutar Web Crawler
echo.
echo 🕷️ Paso 1: Ejecutando Web Crawler...
cd webCrawler
python crawler.py
if errorlevel 1 (
    echo ❌ Error ejecutando el web crawler
    cd ..
    pause
    exit /b 1
)
cd ..

REM Verificar que se crearon los datos en HDFS
echo 🔍 Verificando datos en HDFS...
docker exec namenode hdfs dfs -test -e /user/root/wiki_data/wiki_data.jsonl
if errorlevel 1 (
    echo ❌ No se encontró el archivo wiki_data.jsonl en HDFS
    echo 💡 Verifica que el crawler haya subido los datos correctamente
    pause
    exit /b 1
)
echo ✅ Archivo wiki_data.jsonl encontrado en HDFS

REM 2. Ejecutar análisis de Spark
echo.
echo ⚡ Paso 2: Ejecutando análisis de Spark...
call scripts\run_spark_analysis.bat
if errorlevel 1 (
    echo ❌ Error ejecutando análisis de Spark
    pause
    exit /b 1
)

echo.
echo ✅ Pipeline de datos completado exitosamente
echo 📊 Los datos han sido procesados y están disponibles en MySQL

REM 3. Verificar datos en MySQL
echo 🔍 Verificando datos insertados:
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SELECT COUNT(*) as 'Páginas' FROM Page; SELECT COUNT(*) as 'Palabras' FROM Word; SELECT COUNT(*) as 'Relaciones Palabra-Página' FROM TopWordPages;"

REM 4. Iniciar aplicación web
echo.
echo 🌐 Paso 3: Iniciando aplicación web...
echo 💡 La aplicación estará disponible en: http://localhost:5000
echo ⏹️ Presiona Ctrl+C para detener el servidor
echo.
cd WebLoader
python app.py

echo ✅ Pipeline completo ejecutado
pause